from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from danube.connection_manager import ConnectionManager

INTERNAL_BROKER_HEADER = "x-danube-internal-broker"


class AuthService:
    """Handles JWT token insertion into gRPC request metadata.

    With JWT-first authentication, the client uses a pre-generated JWT token
    (from ``danube-admin security tokens create``) that is sent as
    ``Authorization: Bearer <token>`` on every gRPC request.
    """

    def __init__(self, cnx_manager: ConnectionManager) -> None:
        self._cnx_manager = cnx_manager

    async def attach_token_if_needed(
        self, addr: str
    ) -> Optional[list[tuple[str, str]]]:
        """Return gRPC metadata with auth token, or None if no token configured."""
        token = self._cnx_manager.options.resolve_token()
        metadata: list[tuple[str, str]] = []

        if token:
            metadata.append(("authorization", f"Bearer {token}"))

        internal_broker = self._cnx_manager.options.internal_broker
        if internal_broker:
            metadata.append((INTERNAL_BROKER_HEADER, internal_broker))

        return metadata if metadata else None
