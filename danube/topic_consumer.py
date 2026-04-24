from __future__ import annotations

import asyncio
import itertools
import logging
from typing import TYPE_CHECKING, Optional

import grpc

from danube.errors import UnrecoverableError
from danube.proto import DanubeApi_pb2, DanubeApi_pb2_grpc
from danube.retry_manager import RetryManager, insert_proxy_header

if TYPE_CHECKING:
    from danube.client import DanubeClient

logger = logging.getLogger(__name__)


class TopicConsumer:
    """Low-level consumer bound to a single topic (or topic-partition)."""

    def __init__(
        self,
        client: DanubeClient,
        topic_name: str,
        consumer_name: str,
        subscription: str,
        subscription_type: int,
        key_filters: Optional[list[str]] = None,
        max_retries: int = 0,
        base_backoff_ms: int = 0,
        max_backoff_ms: int = 0,
    ) -> None:
        self.client = client
        self.topic_name = topic_name
        self.consumer_name = consumer_name
        self.consumer_id: int = 0
        self.subscription = subscription
        self.subscription_type = subscription_type
        self.key_filters = key_filters or []
        self._request_id = itertools.count(1)
        self.retry_manager = RetryManager(max_retries, base_backoff_ms, max_backoff_ms)
        self._stub: Optional[DanubeApi_pb2_grpc.ConsumerServiceStub] = None
        self.stop_event = asyncio.Event()
        self._health_task: Optional[asyncio.Task] = None
        # dual-URL + proxy fields
        self.broker_addr: str = client.uri
        self.connect_url: str = client.uri
        self.proxy: bool = False

    def stop(self) -> None:
        self.stop_event.set()
        if self._health_task is not None:
            self._health_task.cancel()
            self._health_task = None

    async def subscribe(self) -> int:
        # Perform an initial topic lookup to discover the owning broker.
        # This sets broker_addr, connect_url, and proxy flag so that
        # proxy routing headers are present from the very first RPC.
        await self._lookup_new_broker()

        attempts = 0
        last_err: Optional[Exception] = None
        while True:
            try:
                return await self._try_subscribe()
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

    async def _try_subscribe(self) -> int:
        await self._connect()

        request = DanubeApi_pb2.ConsumerRequest(
            request_id=next(self._request_id),
            topic_name=self.topic_name,
            consumer_name=self.consumer_name,
            subscription=self.subscription,
            subscription_type=self.subscription_type,
            key_filters=self.key_filters,
        )

        metadata = await self.client.auth_service.attach_token_if_needed(self.connect_url)
        metadata = insert_proxy_header(metadata, self.broker_addr, self.proxy)

        try:
            resp = await self._stub.Subscribe(request, metadata=metadata)
        except grpc.aio.AioRpcError as err:
            if err.code() == grpc.StatusCode.ALREADY_EXISTS:
                raise UnrecoverableError(f"consumer already exists: {err}") from err
            raise

        self.consumer_id = resp.consumer_id

        self._health_task = await self.client.health_check_service.start_health_check(
            self.connect_url,
            self.broker_addr,
            self.proxy,
            DanubeApi_pb2.HealthCheckRequest.Consumer,
            self.consumer_id,
            self.stop_event,
        )

        return self.consumer_id

    async def receive(self):
        """Open a streaming RPC for receiving messages."""
        if self._stub is None:
            raise UnrecoverableError("receive: consumer is not connected")

        request = DanubeApi_pb2.ReceiveRequest(
            request_id=next(self._request_id),
            consumer_id=self.consumer_id,
        )

        metadata = await self.client.auth_service.attach_token_if_needed(self.connect_url)
        metadata = insert_proxy_header(metadata, self.broker_addr, self.proxy)

        return self._stub.ReceiveMessages(request, metadata=metadata)

    async def send_ack(self, request_id: int, msg_id, subscription_name: str):
        """Acknowledge a received message."""
        if self._stub is None:
            raise UnrecoverableError("send_ack: consumer is not connected")

        ack_request = DanubeApi_pb2.AckRequest(
            request_id=request_id,
            msg_id=msg_id,
            subscription_name=subscription_name,
        )

        metadata = await self.client.auth_service.attach_token_if_needed(self.connect_url)
        metadata = insert_proxy_header(metadata, self.broker_addr, self.proxy)

        return await self._stub.Ack(ack_request, metadata=metadata)

    async def send_nack(
        self,
        request_id: int,
        msg_id,
        subscription_name: str,
        delay_ms: Optional[int] = None,
        reason: Optional[str] = None,
    ):
        """Send a negative acknowledgement for a received message."""
        if self._stub is None:
            raise UnrecoverableError("send_nack: consumer is not connected")

        kwargs = dict(
            request_id=request_id,
            msg_id=msg_id,
            subscription_name=subscription_name,
        )
        if delay_ms is not None:
            kwargs["delay_ms"] = delay_ms
        if reason is not None:
            kwargs["reason"] = reason

        nack_request = DanubeApi_pb2.NackRequest(**kwargs)

        metadata = await self.client.auth_service.attach_token_if_needed(self.connect_url)
        metadata = insert_proxy_header(metadata, self.broker_addr, self.proxy)

        return await self._stub.Nack(nack_request, metadata=metadata)

    async def _connect(self) -> None:
        conn = await self.client.connection_manager.get_connection(self.broker_addr, self.connect_url)
        self._stub = DanubeApi_pb2_grpc.ConsumerServiceStub(conn.channel)

    async def _lookup_new_broker(self) -> None:
        try:
            addr = await self.client.lookup_service.handle_lookup(self.connect_url, self.topic_name)
            self.broker_addr = addr.broker_url
            self.connect_url = addr.connect_url
            self.proxy = addr.proxy
        except Exception:
            pass
