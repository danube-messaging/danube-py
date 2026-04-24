from __future__ import annotations

import threading


class MessageRouter:
    """Round-robin and key-based partition selector."""

    def __init__(self, partitions: int) -> None:
        self._partitions = partitions
        self._last = partitions - 1
        self._lock = threading.Lock()

    def round_robin(self) -> int:
        with self._lock:
            next_part = (self._last + 1) % self._partitions
            self._last = next_part
            return next_part

    def key_route(self, routing_key: str) -> int:
        """Route by hashing the routing key to a deterministic partition.

        Ensures all messages with the same key always go to the same partition,
        which is required for per-key ordering on partitioned Key-Shared topics.
        """
        h = _fnv1a_hash(routing_key)
        return h % self._partitions


def _fnv1a_hash(key: str) -> int:
    """FNV-1a 64-bit hash — must match Rust/Go constants."""
    h = 0xCBF29CE484222325
    for byte in key.encode():
        h ^= byte
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h
