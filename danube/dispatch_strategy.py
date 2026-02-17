from __future__ import annotations

from enum import IntEnum

from danube.proto import DanubeApi_pb2


class DispatchStrategy(IntEnum):
    """Message delivery strategy."""
    NON_RELIABLE = 0
    RELIABLE = 1

    def to_proto(self) -> int:
        return int(self.value)
