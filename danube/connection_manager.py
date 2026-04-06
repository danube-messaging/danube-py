from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Optional

import grpc


@dataclass(frozen=True)
class BrokerAddress:
    """Holds connection information for a broker."""
    connect_url: str
    broker_url: str
    proxy: bool


# Type alias for a dynamic token supplier called on every request.
TokenSupplier = Callable[[], str]


@dataclass
class ConnectionOptions:
    """Configures how the client connects to the broker."""
    use_tls: bool = False
    tls_credentials: Optional[grpc.ChannelCredentials] = None
    token: str = ""
    token_supplier: Optional[TokenSupplier] = None
    internal_broker: str = ""

    def resolve_token(self) -> str:
        """Return the current token.

        If a supplier is set, calls it to get a fresh token (enabling runtime
        rotation). Otherwise falls back to the static token.
        """
        if self.token_supplier is not None:
            return self.token_supplier()
        return self.token


class RpcConnection:
    """Wraps an async gRPC channel."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        self.channel = channel


class ConnectionManager:
    """Manages gRPC connections to brokers, keyed by (broker_url, connect_url)."""

    def __init__(self, options: ConnectionOptions) -> None:
        self.options = options
        self._connections: dict[BrokerAddress, RpcConnection] = {}
        self._lock = asyncio.Lock()

    async def get_connection(self, broker_url: str, connect_url: str) -> RpcConnection:
        proxy = broker_url != connect_url
        key = BrokerAddress(connect_url=connect_url, broker_url=broker_url, proxy=proxy)

        async with self._lock:
            conn = self._connections.get(key)
            if conn is not None:
                return conn

            channel = self._create_channel(connect_url)
            conn = RpcConnection(channel)
            self._connections[key] = conn
            return conn

    def _create_channel(self, connect_url: str) -> grpc.aio.Channel:
        # Strip http(s):// prefix – gRPC Python expects host:port
        target = connect_url
        for prefix in ("https://", "http://"):
            if target.startswith(prefix):
                target = target[len(prefix):]
                break

        if self.options.use_tls:
            creds = self.options.tls_credentials or grpc.ssl_channel_credentials()
            return grpc.aio.secure_channel(target, creds)
        else:
            return grpc.aio.insecure_channel(target)
