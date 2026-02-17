from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Optional

import grpc

from danube.proto import DanubeApi_pb2, DanubeApi_pb2_grpc

if TYPE_CHECKING:
    from danube.connection_manager import ConnectionManager

TOKEN_EXPIRY_SECS = 3600


class AuthService:
    """Handles API-key authentication and token caching."""

    def __init__(self, cnx_manager: ConnectionManager) -> None:
        self._cnx_manager = cnx_manager
        self._token: str = ""
        self._expiry: float = 0.0
        self._lock = asyncio.Lock()

    async def authenticate_client(self, addr: str, api_key: str) -> str:
        conn = await self._cnx_manager.get_connection(addr, addr)
        stub = DanubeApi_pb2_grpc.AuthServiceStub(conn.channel)
        resp = await stub.Authenticate(DanubeApi_pb2.AuthRequest(api_key=api_key))
        token = resp.token

        async with self._lock:
            self._token = token
            self._expiry = time.monotonic() + TOKEN_EXPIRY_SECS

        return token

    async def get_valid_token(self, addr: str, api_key: str) -> str:
        async with self._lock:
            token = self._token
            expiry = self._expiry

        if token and time.monotonic() < expiry:
            return token

        return await self.authenticate_client(addr, api_key)

    async def attach_token_if_needed(
        self, api_key: str, addr: str
    ) -> Optional[list[tuple[str, str]]]:
        """Return gRPC metadata with auth token, or None if no api_key."""
        if not api_key:
            return None

        token = await self.get_valid_token(addr, api_key)
        return [("authorization", f"Bearer {token}")]
