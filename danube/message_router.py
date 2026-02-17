from __future__ import annotations

import threading


class MessageRouter:
    """Round-robin partition selector."""

    def __init__(self, partitions: int) -> None:
        self._partitions = partitions
        self._last = partitions - 1
        self._lock = threading.Lock()

    def round_robin(self) -> int:
        with self._lock:
            next_part = (self._last + 1) % self._partitions
            self._last = next_part
            return next_part
