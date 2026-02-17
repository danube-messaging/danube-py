"""Shared fixtures and helpers for integration tests."""

from __future__ import annotations

import asyncio
import os
import time
from typing import AsyncGenerator

import pytest

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from danube import DanubeClientBuilder, DanubeClient
from danube.proto import DanubeApi_pb2

DEFAULT_BROKER_URL = "http://127.0.0.1:6650"


def broker_url() -> str:
    return os.environ.get("DANUBE_BROKER_URL", DEFAULT_BROKER_URL)


def unique_topic(prefix: str) -> str:
    return f"{prefix}-{int(time.time() * 1000)}"


@pytest.fixture
async def client() -> DanubeClient:
    return await DanubeClientBuilder().service_url(broker_url()).build()


async def receive_messages(
    queue: asyncio.Queue,
    n: int,
    timeout: float = 10.0,
) -> list[DanubeApi_pb2.StreamMessage]:
    """Read n messages from the queue within the given timeout."""
    messages = []
    deadline = time.monotonic() + timeout
    for _ in range(n):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"timeout: received only {len(messages)}/{n} messages")
        try:
            msg = await asyncio.wait_for(queue.get(), timeout=remaining)
        except asyncio.TimeoutError:
            raise TimeoutError(f"timeout: received only {len(messages)}/{n} messages")
        messages.append(msg)
    return messages


async def receive_one(
    queue: asyncio.Queue,
    timeout: float = 10.0,
) -> DanubeApi_pb2.StreamMessage:
    msgs = await receive_messages(queue, 1, timeout)
    return msgs[0]
