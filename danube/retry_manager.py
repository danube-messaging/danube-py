from __future__ import annotations

import random
from typing import Optional

import grpc

DEFAULT_MAX_RETRIES = 5
DEFAULT_BASE_BACKOFF_MS = 200
DEFAULT_MAX_BACKOFF_MS = 5000


class RetryManager:
    """Centralizes retry logic and backoff calculation."""

    def __init__(
        self,
        max_retries: int = 0,
        base_backoff_ms: int = 0,
        max_backoff_ms: int = 0,
    ) -> None:
        self.max_retries = max_retries if max_retries > 0 else DEFAULT_MAX_RETRIES
        self.base_backoff_ms = base_backoff_ms if base_backoff_ms > 0 else DEFAULT_BASE_BACKOFF_MS
        self.max_backoff_ms = max_backoff_ms if max_backoff_ms > 0 else DEFAULT_MAX_BACKOFF_MS

    def is_retryable(self, error: Exception) -> bool:
        """Check if a gRPC error is retryable."""
        if isinstance(error, grpc.aio.AioRpcError):
            return error.code() in (
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.RESOURCE_EXHAUSTED,
            )
        return False

    def calculate_backoff(self, attempt: int) -> float:
        """Return backoff duration in seconds with jitter."""
        linear = self.base_backoff_ms * (attempt + 1)
        if linear > self.max_backoff_ms:
            linear = self.max_backoff_ms
        low = max(linear // 2, 1)
        jitter_ms = random.randint(low, linear)
        return jitter_ms / 1000.0


def insert_proxy_header(
    metadata: Optional[list[tuple[str, str]]],
    broker_url: str,
    proxy: bool,
) -> Optional[list[tuple[str, str]]]:
    """Append x-danube-broker-url metadata header when proxy mode is active."""
    if not proxy:
        return metadata
    entry = ("x-danube-broker-url", broker_url)
    if metadata is None:
        return [entry]
    return metadata + [entry]
