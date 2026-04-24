from __future__ import annotations

import asyncio
import itertools
import logging
import time
from typing import TYPE_CHECKING, Optional

import grpc

from danube.dispatch_strategy import DispatchStrategy
from danube.errors import UnrecoverableError
from danube.proto import DanubeApi_pb2, DanubeApi_pb2_grpc
from danube.retry_manager import RetryManager, insert_proxy_header

if TYPE_CHECKING:
    from danube.client import DanubeClient

logger = logging.getLogger(__name__)


class TopicProducer:
    """Low-level producer bound to a single topic (or topic-partition)."""

    def __init__(
        self,
        client: DanubeClient,
        topic: str,
        producer_name: str,
        schema_ref: Optional[DanubeApi_pb2.SchemaReference],
        dispatch_strategy: DispatchStrategy,
        max_retries: int,
        base_backoff_ms: int,
        max_backoff_ms: int,
    ) -> None:
        self.client = client
        self.topic = topic
        self.producer_name = producer_name
        self.producer_id: int = 0
        self._request_id = itertools.count(1)
        self.schema_ref = schema_ref
        self.schema_id: Optional[int] = None
        self.schema_version: Optional[int] = None
        self.dispatch_strategy = dispatch_strategy
        self.retry_manager = RetryManager(max_retries, base_backoff_ms, max_backoff_ms)
        self._stub: Optional[DanubeApi_pb2_grpc.ProducerServiceStub] = None
        self.stop_event = asyncio.Event()
        self._health_task: Optional[asyncio.Task] = None
        # dual-URL + proxy fields
        self.broker_addr: str = client.uri
        self.connect_url: str = client.uri
        self.proxy: bool = False

    async def create(self) -> int:
        # Perform an initial topic lookup to discover the owning broker.
        # This sets broker_addr, connect_url, and proxy flag so that
        # proxy routing headers are present from the very first RPC.
        await self._lookup_new_broker()

        attempts = 0
        last_err: Optional[Exception] = None
        while True:
            try:
                return await self._try_create()
            except Exception as err:
                last_err = err

            if not self.retry_manager.is_retryable(last_err):
                raise last_err

            attempts += 1
            if attempts > self.retry_manager.max_retries:
                raise last_err

            await self._lookup_new_broker()
            backoff = self.retry_manager.calculate_backoff(attempts - 1)
            await asyncio.sleep(backoff)

    async def _try_create(self) -> int:
        await self._connect()

        request = DanubeApi_pb2.ProducerRequest(
            request_id=next(self._request_id),
            producer_name=self.producer_name,
            topic_name=self.topic,
            schema_ref=self.schema_ref,
            producer_access_mode=DanubeApi_pb2.Shared,
            dispatch_strategy=self.dispatch_strategy.to_proto(),
        )

        metadata = await self.client.auth_service.attach_token_if_needed(self.connect_url)
        metadata = insert_proxy_header(metadata, self.broker_addr, self.proxy)

        try:
            resp = await self._stub.CreateProducer(request, metadata=metadata)
        except grpc.aio.AioRpcError as err:
            if err.code() == grpc.StatusCode.ALREADY_EXISTS:
                raise UnrecoverableError(f"producer already exists: {err}") from err
            raise

        self.producer_id = resp.producer_id

        self._health_task = await self.client.health_check_service.start_health_check(
            self.connect_url,
            self.broker_addr,
            self.proxy,
            DanubeApi_pb2.HealthCheckRequest.Producer,
            self.producer_id,
            self.stop_event,
        )

        if self.schema_ref is not None:
            schema_id, schema_version = await self._resolve_schema_metadata()
            self.schema_id = schema_id
            self.schema_version = schema_version

        return self.producer_id

    def stop(self) -> None:
        self.stop_event.set()
        if self._health_task is not None:
            self._health_task.cancel()
            self._health_task = None

    async def send(self, data: bytes, attributes: Optional[dict[str, str]] = None, routing_key: Optional[str] = None) -> int:
        if self._stub is None:
            raise UnrecoverableError("send: producer is not connected")

        if attributes is None:
            attributes = {}

        publish_time = int(time.time() * 1000)

        msg_id = DanubeApi_pb2.MsgID(
            producer_id=self.producer_id,
            topic_name=self.topic,
            broker_addr=self.broker_addr,
        )

        request = DanubeApi_pb2.StreamMessage(
            request_id=next(self._request_id),
            msg_id=msg_id,
            payload=data,
            publish_time=publish_time,
            producer_name=self.producer_name,
            subscription_name="",
            attributes=attributes,
        )
        if self.schema_id is not None:
            request.schema_id = self.schema_id
        if self.schema_version is not None:
            request.schema_version = self.schema_version
        if routing_key is not None:
            request.routing_key = routing_key

        metadata = await self.client.auth_service.attach_token_if_needed(self.connect_url)
        metadata = insert_proxy_header(metadata, self.broker_addr, self.proxy)

        resp = await self._stub.SendMessage(request, metadata=metadata)
        return resp.request_id

    async def _connect(self) -> None:
        conn = await self.client.connection_manager.get_connection(self.broker_addr, self.connect_url)
        self._stub = DanubeApi_pb2_grpc.ProducerServiceStub(conn.channel)

    async def _lookup_new_broker(self) -> None:
        try:
            addr = await self.client.lookup_service.handle_lookup(self.connect_url, self.topic)
            self.broker_addr = addr.broker_url
            self.connect_url = addr.connect_url
            self.proxy = addr.proxy
        except Exception:
            pass

    async def _resolve_schema_metadata(self) -> tuple[int, int]:
        schema_client = self.client.schema()
        ref = self.schema_ref

        if ref is None:
            raise ValueError("schema reference is None")

        version_ref = ref.WhichOneof("version_ref")

        if version_ref == "pinned_version":
            latest = await schema_client.get_latest_schema(ref.subject)
            if ref.pinned_version > latest.version:
                raise ValueError(
                    f"pinned version {ref.pinned_version} does not exist for subject {ref.subject}"
                )
            if ref.pinned_version == latest.version:
                return latest.schema_id, latest.version
            pinned = await schema_client.get_schema_version(latest.schema_id, ref.pinned_version)
            return pinned.schema_id, pinned.version

        elif version_ref == "min_version":
            latest = await schema_client.get_latest_schema(ref.subject)
            if latest.version < ref.min_version:
                raise ValueError(
                    f"latest version {latest.version} does not meet minimum {ref.min_version} "
                    f"for subject {ref.subject}"
                )
            return latest.schema_id, latest.version

        else:
            # use_latest or default
            latest = await schema_client.get_latest_schema(ref.subject)
            return latest.schema_id, latest.version
