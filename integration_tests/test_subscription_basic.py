"""Basic subscription tests: one producer, one consumer, send/receive/ack."""

from __future__ import annotations

import asyncio

import pytest

from danube import DanubeClient, SubType
from integration_tests.conftest import receive_one, unique_topic


async def run_basic_subscription(client: DanubeClient, topic_prefix: str, sub_type: SubType):
    topic = unique_topic(topic_prefix)

    # Producer
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_basic")
        .build()
    )
    await producer.create()

    # Consumer
    consumer = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name("consumer_basic")
        .with_subscription("test_sub_basic")
        .with_subscription_type(sub_type)
        .build()
    )
    await consumer.subscribe()

    try:
        queue = await consumer.receive()
        await asyncio.sleep(0.3)

        payload = b"Hello Danube"
        await producer.send(payload)

        msg = await receive_one(queue, timeout=10.0)
        assert msg.payload == payload, f"payload mismatch: got {msg.payload!r}"

        await consumer.ack(msg)
    finally:
        await producer.close()
        await consumer.close()


@pytest.mark.asyncio
async def test_basic_subscription_shared(client: DanubeClient):
    await run_basic_subscription(client, "/default/sub_basic_shared_py", SubType.SHARED)


@pytest.mark.asyncio
async def test_basic_subscription_exclusive(client: DanubeClient):
    await run_basic_subscription(client, "/default/sub_basic_exclusive_py", SubType.EXCLUSIVE)
