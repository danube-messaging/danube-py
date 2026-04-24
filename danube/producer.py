from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from danube.dispatch_strategy import DispatchStrategy
from danube.errors import DanubeError, UnrecoverableError
from danube.message_router import MessageRouter
from danube.proto import DanubeApi_pb2
from danube.retry_manager import RetryManager
from danube.topic_producer import TopicProducer

if TYPE_CHECKING:
    from danube.client import DanubeClient

logger = logging.getLogger(__name__)


@dataclass
class ProducerOptions:
    """Retry configuration for producers."""
    max_retries: int = 0
    base_backoff_ms: int = 0
    max_backoff_ms: int = 0


class Producer:
    """High-level producer supporting partitioned and non-partitioned topics."""

    def __init__(
        self,
        client: DanubeClient,
        topic_name: str,
        producer_name: str,
        partitions: int,
        schema_ref: Optional[DanubeApi_pb2.SchemaReference],
        dispatch_strategy: DispatchStrategy,
        options: ProducerOptions,
    ) -> None:
        self._client = client
        self._topic_name = topic_name
        self._producer_name = producer_name
        self._partitions = partitions
        self._schema_ref = schema_ref
        self._dispatch_strategy = dispatch_strategy
        self._options = options
        self._producers: list[TopicProducer] = []
        self._message_router: Optional[MessageRouter] = None
        self._lock = asyncio.Lock()

    async def create(self) -> None:
        """Create the producer and register with the broker(s)."""
        async with self._lock:
            if self._partitions == 0:
                tp = TopicProducer(
                    client=self._client,
                    topic=self._topic_name,
                    producer_name=self._producer_name,
                    schema_ref=self._schema_ref,
                    dispatch_strategy=self._dispatch_strategy,
                    max_retries=self._options.max_retries,
                    base_backoff_ms=self._options.base_backoff_ms,
                    max_backoff_ms=self._options.max_backoff_ms,
                )
                await tp.create()
                self._producers = [tp]
            else:
                self._message_router = MessageRouter(self._partitions)
                tasks = []
                for pid in range(self._partitions):
                    topic = f"{self._topic_name}-part-{pid}"
                    name = f"{self._producer_name}-{pid}"
                    tp = TopicProducer(
                        client=self._client,
                        topic=topic,
                        producer_name=name,
                        schema_ref=self._schema_ref,
                        dispatch_strategy=self._dispatch_strategy,
                        max_retries=self._options.max_retries,
                        base_backoff_ms=self._options.base_backoff_ms,
                        max_backoff_ms=self._options.max_backoff_ms,
                    )
                    tasks.append((pid, tp))

                producers: list[Optional[TopicProducer]] = [None] * self._partitions
                async def _create_one(idx: int, tp: TopicProducer):
                    await tp.create()
                    producers[idx] = tp

                await asyncio.gather(*[_create_one(i, t) for i, t in tasks])
                self._producers = producers  # type: ignore[assignment]

    async def send(self, data: bytes, attributes: Optional[dict[str, str]] = None) -> int:
        """Send a message, returns the sequence/request ID."""
        async with self._lock:
            partition_id = self._select_partition()

            if partition_id >= len(self._producers):
                raise DanubeError("partition ID out of range")

            retry_manager = RetryManager(
                self._options.max_retries,
                self._options.base_backoff_ms,
                self._options.max_backoff_ms,
            )
            attempts = 0

        # Release lock for the actual send + retry loop
        while True:
            try:
                return await self._producers[partition_id].send(data, attributes)
            except UnrecoverableError:
                await self._recreate_producer(partition_id)
                attempts = 0
                continue
            except Exception as err:
                if retry_manager.is_retryable(err):
                    attempts += 1
                    if attempts > retry_manager.max_retries:
                        await self._lookup_and_recreate(partition_id, err)
                        attempts = 0
                        continue
                    backoff = retry_manager.calculate_backoff(attempts - 1)
                    await asyncio.sleep(backoff)
                    continue
                raise

    def _select_partition(self) -> int:
        if self._partitions > 0 and self._message_router is not None:
            return self._message_router.round_robin()
        return 0

    def _select_partition_for_key(self, routing_key: str) -> int:
        """Select a partition by hashing the routing key.

        Ensures all messages with the same key go to the same partition.
        For non-partitioned topics, returns 0.
        """
        if self._partitions > 0 and self._message_router is not None:
            return self._message_router.key_route(routing_key)
        return 0

    async def send_with_key(
        self,
        data: bytes,
        attributes: Optional[dict[str, str]],
        routing_key: str,
    ) -> int:
        """Send a message with a routing key for KEY_SHARED subscriptions.

        For partitioned topics: hashes the routing key to a specific partition,
        ensuring all messages with the same key go to the same partition's WAL.
        For non-partitioned topics: simply tags the routing key on the message.

        All messages with the same routing key are guaranteed to be delivered
        to the same consumer, in order, within a KEY_SHARED subscription.
        """
        async with self._lock:
            partition_id = self._select_partition_for_key(routing_key)

            if partition_id >= len(self._producers):
                raise DanubeError("partition ID out of range")

            retry_manager = RetryManager(
                self._options.max_retries,
                self._options.base_backoff_ms,
                self._options.max_backoff_ms,
            )
            attempts = 0

        while True:
            try:
                return await self._producers[partition_id].send(data, attributes, routing_key)
            except UnrecoverableError:
                await self._recreate_producer(partition_id)
                attempts = 0
                continue
            except Exception as err:
                if retry_manager.is_retryable(err):
                    attempts += 1
                    if attempts > retry_manager.max_retries:
                        await self._lookup_and_recreate(partition_id, err)
                        attempts = 0
                        continue
                    backoff = retry_manager.calculate_backoff(attempts - 1)
                    await asyncio.sleep(backoff)
                    continue
                raise

    async def close(self) -> None:
        """Stop all background health check tasks for this producer."""
        for tp in self._producers:
            tp.stop()

    async def _recreate_producer(self, partition_id: int) -> None:
        await self._producers[partition_id].create()

    async def _lookup_and_recreate(self, partition_id: int, original_error: Exception) -> None:
        producer = self._producers[partition_id]
        try:
            addr = await producer.client.lookup_service.handle_lookup(
                producer.connect_url, producer.topic
            )
        except Exception:
            raise original_error

        producer.broker_addr = addr.broker_url
        producer.connect_url = addr.connect_url
        producer.proxy = addr.proxy
        await producer.create()


class ProducerBuilder:
    """Builder for creating Producer instances."""

    def __init__(self, client: DanubeClient) -> None:
        self._client = client
        self._topic: str = ""
        self._producer_name: str = ""
        self._partitions: int = 0
        self._schema_ref: Optional[DanubeApi_pb2.SchemaReference] = None
        self._dispatch_strategy: DispatchStrategy = DispatchStrategy.NON_RELIABLE
        self._options = ProducerOptions()

    def with_topic(self, topic: str) -> ProducerBuilder:
        self._topic = topic
        return self

    def with_name(self, name: str) -> ProducerBuilder:
        self._producer_name = name
        return self

    def with_schema_subject(self, subject: str) -> ProducerBuilder:
        self._schema_ref = DanubeApi_pb2.SchemaReference(
            subject=subject, use_latest=True
        )
        return self

    def with_schema_version(self, subject: str, version: int) -> ProducerBuilder:
        self._schema_ref = DanubeApi_pb2.SchemaReference(
            subject=subject, pinned_version=version
        )
        return self

    def with_schema_min_version(self, subject: str, min_version: int) -> ProducerBuilder:
        self._schema_ref = DanubeApi_pb2.SchemaReference(
            subject=subject, min_version=min_version
        )
        return self

    def with_dispatch_strategy(self, strategy: DispatchStrategy) -> ProducerBuilder:
        self._dispatch_strategy = strategy
        return self

    def with_partitions(self, partitions: int) -> ProducerBuilder:
        self._partitions = partitions
        return self

    def with_options(self, options: ProducerOptions) -> ProducerBuilder:
        self._options = options
        return self

    def build(self) -> Producer:
        if not self._topic:
            raise ValueError("topic must be set")
        if not self._producer_name:
            raise ValueError("producer name must be set")

        return Producer(
            client=self._client,
            topic_name=self._topic,
            producer_name=self._producer_name,
            partitions=self._partitions,
            schema_ref=self._schema_ref,
            dispatch_strategy=self._dispatch_strategy,
            options=self._options,
        )
