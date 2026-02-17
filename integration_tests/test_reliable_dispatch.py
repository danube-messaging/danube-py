"""Reliable dispatch tests: WAL-backed delivery with payload integrity checks."""

from __future__ import annotations

import asyncio

import pytest

from danube import DanubeClient, DispatchStrategy, SubType
from integration_tests.conftest import unique_topic


async def run_reliable_basic(client: DanubeClient, topic_prefix: str, sub_type: SubType):
    topic = unique_topic(topic_prefix)

    # Producer with reliable dispatch
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_reliable_basic")
        .with_dispatch_strategy(DispatchStrategy.RELIABLE)
        .build()
    )
    await producer.create()

    # Consumer
    consumer = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name("cons_rel_basic")
        .with_subscription("rel_sub_basic")
        .with_subscription_type(sub_type)
        .build()
    )
    await consumer.subscribe()

    try:
        queue = await consumer.receive()
        await asyncio.sleep(0.4)

        # Generate a non-trivial payload (~1 KB)
        blob_data = b"danube-reliable-test-payload!" * 37
        message_count = 20

        for i in range(message_count):
            await producer.send(blob_data)

        # For reliable dispatch the broker waits for an ack before sending the next
        # message, so we must ack inline during receive (matching Rust/Go test behavior).
        for i in range(message_count):
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=15.0)
            except asyncio.TimeoutError:
                pytest.fail(f"timeout: received only {i}/{message_count} messages")

            assert msg.payload == blob_data, (
                f"message {i} payload mismatch: got {len(msg.payload)} bytes, "
                f"want {len(blob_data)} bytes"
            )
            await consumer.ack(msg)
    finally:
        await producer.close()
        await consumer.close()


@pytest.mark.asyncio
async def test_reliable_dispatch_exclusive(client: DanubeClient):
    await run_reliable_basic(client, "/default/reliable_basic_exclusive_py", SubType.EXCLUSIVE)


@pytest.mark.asyncio
async def test_reliable_dispatch_shared(client: DanubeClient):
    await run_reliable_basic(client, "/default/reliable_basic_shared_py", SubType.SHARED)
