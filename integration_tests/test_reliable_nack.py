"""Reliable nack tests: verify that nacking a message triggers redelivery."""

from __future__ import annotations

import asyncio

import pytest

from danube import DanubeClient, DispatchStrategy, SubType
from integration_tests.conftest import unique_topic


async def run_reliable_nack(client: DanubeClient, topic_prefix: str, sub_type: SubType):
    topic = unique_topic(topic_prefix)

    # Producer with reliable dispatch
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_reliable_nack")
        .with_dispatch_strategy(DispatchStrategy.RELIABLE)
        .build()
    )
    await producer.create()

    # Consumer
    consumer = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name("cons_rel_nack")
        .with_subscription("rel_sub_nack")
        .with_subscription_type(sub_type)
        .build()
    )
    await consumer.subscribe()

    try:
        queue = await consumer.receive()
        await asyncio.sleep(0.4)

        payload = b"nack-redelivery-test"
        await producer.send(payload)

        # Receive the first delivery
        try:
            first_msg = await asyncio.wait_for(queue.get(), timeout=10.0)
        except asyncio.TimeoutError:
            pytest.fail("timeout waiting for first delivery")

        assert first_msg.payload == payload, (
            f"first delivery payload mismatch: got {first_msg.payload!r}, want {payload!r}"
        )

        # Nack the message with zero delay to trigger immediate redelivery
        await consumer.nack(first_msg, delay_ms=0, reason="testing nack redelivery")

        # Receive the redelivered message
        try:
            redelivered = await asyncio.wait_for(queue.get(), timeout=10.0)
        except asyncio.TimeoutError:
            pytest.fail("timeout waiting for redelivered message after nack")

        assert redelivered.payload == payload, (
            f"redelivered payload mismatch: got {redelivered.payload!r}, want {payload!r}"
        )
        assert redelivered.msg_id.topic_offset == first_msg.msg_id.topic_offset, (
            f"redelivered message has different offset: "
            f"got {redelivered.msg_id.topic_offset}, want {first_msg.msg_id.topic_offset}"
        )

        # Ack the redelivered message to complete the cycle
        await consumer.ack(redelivered)
    finally:
        await producer.close()
        await consumer.close()


@pytest.mark.asyncio
async def test_reliable_nack_exclusive(client: DanubeClient):
    await run_reliable_nack(client, "/default/reliable_nack_exclusive_py", SubType.EXCLUSIVE)


@pytest.mark.asyncio
async def test_reliable_nack_shared(client: DanubeClient):
    await run_reliable_nack(client, "/default/reliable_nack_shared_py", SubType.SHARED)
