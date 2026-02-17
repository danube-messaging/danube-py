"""Partitioned topic tests: 3 partitions, verify all messages received with partition coverage."""

from __future__ import annotations

import asyncio

import pytest

from danube import DanubeClient, SubType
from integration_tests.conftest import receive_messages, unique_topic


async def run_partitioned_basic(client: DanubeClient, topic_prefix: str, sub_type: SubType):
    topic = unique_topic(topic_prefix)
    partitions = 3

    # Partitioned producer
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_part_basic")
        .with_partitions(partitions)
        .build()
    )
    await producer.create()

    # Consumer
    consumer = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name("cons_part_basic")
        .with_subscription("sub_part_basic")
        .with_subscription_type(sub_type)
        .build()
    )
    await consumer.subscribe()

    try:
        queue = await consumer.receive()
        await asyncio.sleep(0.3)

        expected = ["Hello Danube 1", "Hello Danube 2", "Hello Danube 3"]
        for body in expected:
            await producer.send(body.encode())

        messages = await receive_messages(queue, len(expected), timeout=10.0)

        # Verify all payloads received
        received = set()
        parts_seen = set()
        for msg in messages:
            received.add(msg.payload.decode())
            parts_seen.add(msg.msg_id.topic_name)
            await consumer.ack(msg)

        for body in expected:
            assert body in received, f"missing message {body!r}"

        # Verify partition coverage
        for i in range(partitions):
            part_name = f"{topic}-part-{i}"
            assert part_name in parts_seen, f"missing partition {part_name}"
    finally:
        await producer.close()
        await consumer.close()


@pytest.mark.asyncio
async def test_partitioned_basic_exclusive(client: DanubeClient):
    await run_partitioned_basic(client, "/default/part_basic_excl_py", SubType.EXCLUSIVE)


@pytest.mark.asyncio
async def test_partitioned_basic_shared(client: DanubeClient):
    await run_partitioned_basic(client, "/default/part_basic_shared_py", SubType.SHARED)
