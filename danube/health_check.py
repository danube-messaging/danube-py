from __future__ import annotations

import asyncio
import itertools
import logging
from typing import TYPE_CHECKING

from danube.proto import DanubeApi_pb2, DanubeApi_pb2_grpc
from danube.retry_manager import insert_proxy_header

if TYPE_CHECKING:
    from danube.auth_service import AuthService
    from danube.connection_manager import ConnectionManager

logger = logging.getLogger(__name__)

HEALTH_CHECK_INTERVAL_SECS = 5


class HealthCheckService:
    """Periodic health check that detects broker-initiated topic closure."""

    def __init__(self, cnx_manager: ConnectionManager, auth_service: AuthService) -> None:
        self._cnx_manager = cnx_manager
        self._auth_service = auth_service
        self._request_id = itertools.count(1)

    async def start_health_check(
        self,
        connect_url: str,
        broker_addr: str,
        proxy: bool,
        client_type: int,
        client_id: int,
        stop_event: asyncio.Event,
    ) -> asyncio.Task:
        conn = await self._cnx_manager.get_connection(broker_addr, connect_url)
        stub = DanubeApi_pb2_grpc.HealthCheckStub(conn.channel)

        async def _loop() -> None:
            while not stop_event.is_set():
                try:
                    request = DanubeApi_pb2.HealthCheckRequest(
                        request_id=next(self._request_id),
                        client=client_type,
                        id=client_id,
                    )
                    metadata = await self._auth_service.attach_token_if_needed(connect_url)
                    metadata = insert_proxy_header(metadata, broker_addr, proxy)

                    response = await stub.HealthCheck(request, metadata=metadata)
                    if response.status == DanubeApi_pb2.HealthCheckResponse.CLOSE:
                        stop_event.set()
                        return
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    logger.debug("health check error: %s", exc)
                    return

                try:
                    await asyncio.sleep(HEALTH_CHECK_INTERVAL_SECS)
                except asyncio.CancelledError:
                    return

        return asyncio.create_task(_loop())
