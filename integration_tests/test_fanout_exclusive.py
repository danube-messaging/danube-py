"""Fan-out with Exclusive subscriptions: each consumer gets ALL messages."""

from __future__ import annotations

import asyncio

import pytest

from danube import DanubeClient, SubType
from integration_tests.conftest import unique_topic


@pytest.mark.asyncio
async def test_fanout_exclusive(client: DanubeClient):
    topic = unique_topic("/default/fanout_exclusive_py")

    # Producer
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_fanout_exclusive")
        .build()
    )
    await producer.create()

    # 3 consumers, each with unique Exclusive subscription
    consumers = []
    queues = []
    for i in range(3):
        cons = (
            client.new_consumer()
            .with_topic(topic)
            .with_consumer_name(f"fanout-cons-{i}")
            .with_subscription(f"fanout-sub-{i}")
            .with_subscription_type(SubType.EXCLUSIVE)
            .build()
        )
        await cons.subscribe()
        queue = await cons.receive()
        consumers.append(cons)
        queues.append(queue)

    await asyncio.sleep(0.4)

    try:
        total = 24
        expected_payloads = [f"m{i}" for i in range(total)]

        # Send messages
        for body in expected_payloads:
            await producer.send(body.encode())

        # Collect per-consumer receipts concurrently
        per_consumer: dict[int, set[str]] = {i: set() for i in range(3)}

        async def collect(idx: int, queue: asyncio.Queue):
            while len(per_consumer[idx]) < total:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=20.0)
                except asyncio.TimeoutError:
                    break
                per_consumer[idx].add(msg.payload.decode())
                await consumers[idx].ack(msg)

        await asyncio.gather(*[collect(i, q) for i, q in enumerate(queues)])

        # Assert every consumer got every message
        for i in range(3):
            assert len(per_consumer[i]) == total, (
                f"fanout-cons-{i} received {len(per_consumer[i])}/{total}"
            )
            for body in expected_payloads:
                assert body in per_consumer[i], (
                    f"fanout-cons-{i} missing message {body}"
                )
    finally:
        await producer.close()
        for cons in consumers:
            await cons.close()
