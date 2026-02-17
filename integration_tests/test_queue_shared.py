"""Queue semantics with Shared subscriptions: messages distributed round-robin."""

from __future__ import annotations

import asyncio

import pytest

from danube import DanubeClient, SubType
from integration_tests.conftest import unique_topic


@pytest.mark.asyncio
async def test_queue_shared_round_robin(client: DanubeClient):
    topic = unique_topic("/default/queue_shared_py")

    # Producer
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_queue_shared")
        .build()
    )
    await producer.create()

    # 3 consumers, same subscription name
    sub_name = "queue-shared-sub"
    consumers = []
    queues = []
    for i in range(3):
        cons = (
            client.new_consumer()
            .with_topic(topic)
            .with_consumer_name(f"queue-cons-{i}")
            .with_subscription(sub_name)
            .with_subscription_type(SubType.SHARED)
            .build()
        )
        await cons.subscribe()
        queue = await cons.receive()
        consumers.append(cons)
        queues.append(queue)

    await asyncio.sleep(0.4)

    try:
        total = 36
        counts: dict[int, int] = {i: 0 for i in range(3)}
        received = 0
        done = asyncio.Event()

        async def collect(idx: int, queue: asyncio.Queue):
            nonlocal received
            while not done.is_set():
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                await consumers[idx].ack(msg)
                counts[idx] += 1
                received += 1
                if received >= total:
                    done.set()
                    return

        # Start collectors
        tasks = [asyncio.create_task(collect(i, q)) for i, q in enumerate(queues)]

        # Send messages
        for i in range(total):
            await producer.send(f"m{i}".encode())

        # Wait for all messages
        try:
            await asyncio.wait_for(done.wait(), timeout=15.0)
        except asyncio.TimeoutError:
            pytest.fail(f"timeout: received {received}/{total} messages")

        for task in tasks:
            task.cancel()

        # Assert near round-robin: each consumer should get total/3
        expected = total // 3
        for i in range(3):
            assert counts[i] == expected, (
                f"queue-cons-{i} received {counts[i]} messages, expected {expected}"
            )
    finally:
        await producer.close()
        for cons in consumers:
            await cons.close()
