from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Optional

from danube.errors import DanubeError, UnrecoverableError
from danube.proto import DanubeApi_pb2
from danube.retry_manager import RetryManager
from danube.topic_consumer import TopicConsumer

if TYPE_CHECKING:
    from danube.client import DanubeClient

logger = logging.getLogger(__name__)

RECEIVE_CHANNEL_BUFFER = 100
GRACEFUL_CLOSE_DELAY_SECS = 0.1


class SubType(IntEnum):
    """Subscription type."""
    EXCLUSIVE = 0
    SHARED = 1
    FAILOVER = 2


@dataclass
class ConsumerOptions:
    """Retry configuration for consumers."""
    max_retries: int = 0
    base_backoff_ms: int = 0
    max_backoff_ms: int = 0


class Consumer:
    """High-level consumer supporting partitioned and non-partitioned topics."""

    def __init__(
        self,
        client: DanubeClient,
        topic_name: str,
        consumer_name: str,
        subscription: str,
        subscription_type: SubType,
        options: ConsumerOptions,
    ) -> None:
        self._client = client
        self._topic_name = topic_name
        self._consumer_name = consumer_name
        self._subscription = subscription
        self._subscription_type = subscription_type
        self._options = options
        self._consumers: dict[str, TopicConsumer] = {}
        self._shutdown = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def subscribe(self) -> None:
        """Subscribe to all partitions of the topic."""
        partitions = await self._client.lookup_service.topic_partitions(
            self._client.uri, self._topic_name
        )

        async def _subscribe_one(partition: str) -> tuple[str, TopicConsumer]:
            tc = TopicConsumer(
                client=self._client,
                topic_name=partition,
                consumer_name=self._consumer_name,
                subscription=self._subscription,
                subscription_type=int(self._subscription_type),
                max_retries=self._options.max_retries,
                base_backoff_ms=self._options.base_backoff_ms,
                max_backoff_ms=self._options.max_backoff_ms,
            )
            await tc.subscribe()
            return partition, tc

        results = await asyncio.gather(*[_subscribe_one(p) for p in partitions])
        self._consumers = {name: tc for name, tc in results}

        if not self._consumers:
            raise DanubeError("no partitions found")

    async def receive(self) -> asyncio.Queue[DanubeApi_pb2.StreamMessage]:
        """Start receiving messages from all partitions. Returns an asyncio.Queue."""
        queue: asyncio.Queue[DanubeApi_pb2.StreamMessage] = asyncio.Queue(
            maxsize=RECEIVE_CHANNEL_BUFFER
        )

        retry_manager = RetryManager(
            self._options.max_retries,
            self._options.base_backoff_ms,
            self._options.max_backoff_ms,
        )

        for tc in self._consumers.values():
            task = asyncio.create_task(
                _partition_receive_loop(tc, queue, retry_manager, self._shutdown)
            )
            self._tasks.append(task)

        return queue

    async def ack(self, message: DanubeApi_pb2.StreamMessage) -> None:
        """Acknowledge a received message."""
        topic_name = message.msg_id.topic_name
        tc = self._consumers.get(topic_name)
        if tc is not None:
            await tc.send_ack(message.request_id, message.msg_id, self._subscription)

    async def close(self) -> None:
        """Gracefully stop all receive tasks and background activities."""
        self._shutdown.set()
        for tc in self._consumers.values():
            tc.stop()
        for task in self._tasks:
            task.cancel()
        await asyncio.sleep(GRACEFUL_CLOSE_DELAY_SECS)


async def _partition_receive_loop(
    consumer: TopicConsumer,
    queue: asyncio.Queue,
    retry_manager: RetryManager,
    shutdown: asyncio.Event,
) -> None:
    attempts = 0

    while not shutdown.is_set():
        try:
            stream = await consumer.receive()
        except UnrecoverableError:
            try:
                await consumer.subscribe()
                attempts = 0
                continue
            except Exception:
                return
        except Exception as err:
            if retry_manager.is_retryable(err):
                attempts += 1
                if attempts > retry_manager.max_retries:
                    try:
                        await consumer.subscribe()
                        attempts = 0
                        continue
                    except Exception:
                        return
                backoff = retry_manager.calculate_backoff(attempts - 1)
                await asyncio.sleep(backoff)
                continue
            else:
                logger.error("non-retryable error in consumer receive: %s", err)
                return

        # Stream opened successfully
        attempts = 0
        try:
            async for message in stream:
                if shutdown.is_set():
                    return
                if consumer.stop_event.is_set():
                    break
                await queue.put(message)
        except Exception as exc:
            logger.warning("error receiving message: %s", exc)

        if shutdown.is_set():
            return

        # Broker signaled close via health check
        if consumer.stop_event.is_set():
            consumer.stop_event.clear()
            logger.warning("broker signaled topic close, triggering resubscription")
            try:
                await consumer.subscribe()
                continue
            except Exception as exc:
                logger.error("resubscription failed after broker close signal: %s", exc)
                return


class ConsumerBuilder:
    """Builder for creating Consumer instances."""

    def __init__(self, client: DanubeClient) -> None:
        self._client = client
        self._topic: str = ""
        self._consumer_name: str = ""
        self._subscription: str = ""
        self._subscription_type: SubType = SubType.SHARED
        self._options = ConsumerOptions()

    def with_topic(self, topic: str) -> ConsumerBuilder:
        self._topic = topic
        return self

    def with_consumer_name(self, name: str) -> ConsumerBuilder:
        self._consumer_name = name
        return self

    def with_subscription(self, subscription: str) -> ConsumerBuilder:
        self._subscription = subscription
        return self

    def with_subscription_type(self, sub_type: SubType) -> ConsumerBuilder:
        self._subscription_type = sub_type
        return self

    def with_options(self, options: ConsumerOptions) -> ConsumerBuilder:
        self._options = options
        return self

    def build(self) -> Consumer:
        if not self._topic or not self._consumer_name or not self._subscription:
            raise ValueError("topic, consumer_name, and subscription are required")

        return Consumer(
            client=self._client,
            topic_name=self._topic,
            consumer_name=self._consumer_name,
            subscription=self._subscription,
            subscription_type=self._subscription_type,
            options=self._options,
        )
