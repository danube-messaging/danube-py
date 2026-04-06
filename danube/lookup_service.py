from __future__ import annotations

import itertools
from typing import TYPE_CHECKING

from danube.connection_manager import BrokerAddress
from danube.errors import LookupError
from danube.proto import DanubeApi_pb2, DanubeApi_pb2_grpc
from danube.retry_manager import insert_proxy_header

if TYPE_CHECKING:
    from danube.auth_service import AuthService
    from danube.connection_manager import ConnectionManager


class LookupService:
    """Discovers broker addresses for topics."""

    def __init__(self, cnx_manager: ConnectionManager, auth_service: AuthService) -> None:
        self._cnx_manager = cnx_manager
        self._auth_service = auth_service
        self._request_id = itertools.count(1)

    async def lookup_topic(self, addr: str, topic: str) -> DanubeApi_pb2.TopicLookupResponse:
        conn = await self._cnx_manager.get_connection(addr, addr)
        stub = DanubeApi_pb2_grpc.DiscoveryStub(conn.channel)

        metadata = await self._auth_service.attach_token_if_needed(addr)

        request = DanubeApi_pb2.TopicLookupRequest(
            request_id=next(self._request_id),
            topic=topic,
        )
        return await stub.TopicLookup(request, metadata=metadata)

    async def topic_partitions(self, addr: str, topic: str) -> list[str]:
        conn = await self._cnx_manager.get_connection(addr, addr)
        stub = DanubeApi_pb2_grpc.DiscoveryStub(conn.channel)

        metadata = await self._auth_service.attach_token_if_needed(addr)

        request = DanubeApi_pb2.TopicLookupRequest(
            request_id=next(self._request_id),
            topic=topic,
        )
        response = await stub.TopicPartitions(request, metadata=metadata)
        return list(response.partitions)

    async def handle_lookup(self, addr: str, topic: str) -> BrokerAddress:
        response = await self.lookup_topic(addr, topic)
        resp_type = response.response_type

        if resp_type in (
            DanubeApi_pb2.TopicLookupResponse.Redirect,
            DanubeApi_pb2.TopicLookupResponse.Connect,
        ):
            return BrokerAddress(
                connect_url=response.connect_url,
                broker_url=response.broker_url,
                proxy=response.proxy,
            )
        elif resp_type == DanubeApi_pb2.TopicLookupResponse.Failed:
            raise LookupError("topic lookup failed: topic may not exist or cluster is unavailable")
        else:
            raise LookupError("unknown lookup type")
