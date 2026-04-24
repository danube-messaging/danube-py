"""Key-Shared subscription integration tests.

Tests:
- test_key_shared_basic: key routing consistency (same key → same consumer)
- test_key_shared_filtering: deterministic filter routing with explicit filters on both consumers
"""

from __future__ import annotations

import asyncio

import pytest

from danube import DanubeClient, DispatchStrategy, SubType
from integration_tests.conftest import unique_topic


@pytest.mark.asyncio
async def test_key_shared_basic(client: DanubeClient):
    """Verify that Key-Shared dispatch routes messages with the same routing key
    to the same consumer consistently."""
    topic = unique_topic("/default/key_shared_basic_py")

    # Producer with reliable dispatch
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_ks_basic")
        .with_dispatch_strategy(DispatchStrategy.RELIABLE)
        .build()
    )
    await producer.create()

    # Two consumers sharing the same Key-Shared subscription
    consumer1 = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name("cons_ks_1")
        .with_subscription("ks_sub_basic")
        .with_subscription_type(SubType.KEY_SHARED)
        .build()
    )
    await consumer1.subscribe()

    consumer2 = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name("cons_ks_2")
        .with_subscription("ks_sub_basic")
        .with_subscription_type(SubType.KEY_SHARED)
        .build()
    )
    await consumer2.subscribe()

    try:
        queue1 = await consumer1.receive()
        queue2 = await consumer2.receive()

        await asyncio.sleep(0.5)

        # Send messages with two different keys
        message_count = 10
        for i in range(message_count):
            key = "keyA" if i % 2 == 0 else "keyB"
            payload = f"msg-{i}-key-{key}"
            await producer.send_with_key(payload.encode(), None, key)

        # Collect messages from both consumers, acking inline.
        # Key-Shared dispatch has per-key in-flight limits — the broker won't
        # send the next message for the same key until the current one is acked.
        entries = []  # list of (msg, consumer_id)
        deadline = asyncio.get_event_loop().time() + 15.0

        while len(entries) < message_count:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                pytest.fail(f"timeout: collected only {len(entries)}/{message_count} messages")

            # Wait on both queues concurrently
            done, _ = await asyncio.wait(
                [
                    asyncio.create_task(queue1.get()),
                    asyncio.create_task(queue2.get()),
                ],
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not done:
                pytest.fail(f"timeout: collected only {len(entries)}/{message_count} messages")

            for task in done:
                msg = task.result()
                # Determine which consumer got it by checking topic consumer mapping
                topic_name = msg.msg_id.topic_name
                if topic_name in consumer1._consumers:
                    await consumer1.ack(msg)
                    entries.append((msg, 1))
                else:
                    await consumer2.ack(msg)
                    entries.append((msg, 2))

            # Cancel pending tasks from the other queue
            for task in _ :
                task.cancel()

        # Verify: all messages with the same key went to the same consumer
        key_to_consumer: dict[str, int] = {}
        for msg, consumer_id in entries:
            key = msg.routing_key if msg.HasField("routing_key") else ""
            if key in key_to_consumer:
                assert key_to_consumer[key] == consumer_id, (
                    f"key {key!r} was routed to consumer {key_to_consumer[key]} "
                    f"and {consumer_id} — expected same consumer"
                )
            else:
                key_to_consumer[key] = consumer_id

        assert len(entries) == message_count

    finally:
        await producer.close()
        await consumer1.close()
        await consumer2.close()


@pytest.mark.asyncio
async def test_key_shared_filtering(client: DanubeClient):
    """Verify that key filters restrict which messages a consumer receives.

    Both consumers use explicit filters so each key has exactly one eligible
    consumer, making routing deterministic regardless of hash ring layout.
    """
    topic = unique_topic("/default/key_shared_filter_py")

    # Producer with reliable dispatch
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_ks_filter")
        .with_dispatch_strategy(DispatchStrategy.RELIABLE)
        .build()
    )
    await producer.create()

    # Consumer A: only receives "payment" keys
    consumer_a = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name("cons_ks_payments")
        .with_subscription("ks_sub_filter")
        .with_subscription_type(SubType.KEY_SHARED)
        .with_key_filter("payment")
        .build()
    )
    await consumer_a.subscribe()

    # Consumer B: receives "ship*" and "invoice" keys
    consumer_b = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name("cons_ks_logistics")
        .with_subscription("ks_sub_filter")
        .with_subscription_type(SubType.KEY_SHARED)
        .with_key_filter("ship*")
        .with_key_filter("invoice")
        .build()
    )
    await consumer_b.subscribe()

    try:
        queue_a = await consumer_a.receive()
        queue_b = await consumer_b.receive()

        await asyncio.sleep(0.5)

        # Send messages with different keys
        # "payment" → only A eligible, "shipping"/"invoice" → only B eligible
        keys = ["payment", "shipping", "payment", "invoice", "payment"]
        for i, key in enumerate(keys):
            payload = f"event-{i}-{key}"
            await producer.send_with_key(payload.encode(), None, key)

        # Collect all messages, acking inline to unblock per-key dispatch
        keys_on_a: list[str] = []
        keys_on_b: list[str] = []
        all_count = 0
        deadline = asyncio.get_event_loop().time() + 15.0

        while all_count < len(keys):
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                pytest.fail(f"timeout: collected only {all_count}/{len(keys)} messages")

            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(queue_a.get()),
                    asyncio.create_task(queue_b.get()),
                ],
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not done:
                pytest.fail(f"timeout: collected only {all_count}/{len(keys)} messages")

            for task in done:
                msg = task.result()
                topic_name = msg.msg_id.topic_name
                key = msg.routing_key if msg.HasField("routing_key") else ""
                if topic_name in consumer_a._consumers:
                    await consumer_a.ack(msg)
                    keys_on_a.append(key)
                else:
                    await consumer_b.ack(msg)
                    keys_on_b.append(key)
                all_count += 1

            for task in pending:
                task.cancel()

        # Verify: consumer A should only have "payment" keys
        for k in keys_on_a:
            assert k == "payment", f"consumer A (filter: payment) received key {k!r}"

        # Verify: consumer B should only have "shipping" or "invoice" keys
        for k in keys_on_b:
            assert k in ("shipping", "invoice"), (
                f"consumer B (filter: ship*, invoice) received key {k!r}"
            )

        # We sent 3 "payment" + 1 "shipping" + 1 "invoice"
        assert len(keys_on_a) == 3, f"expected 3 messages on consumer A, got {len(keys_on_a)}"
        assert len(keys_on_b) == 2, f"expected 2 messages on consumer B, got {len(keys_on_b)}"

    finally:
        await producer.close()
        await consumer_a.close()
        await consumer_b.close()
