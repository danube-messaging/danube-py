"""Schema version tests: pin to version, latest version resolution."""

from __future__ import annotations

import asyncio

import pytest

from danube import DanubeClient, SchemaType, SubType
from integration_tests.conftest import receive_one, unique_topic


@pytest.mark.asyncio
async def test_producer_pin_to_version(client: DanubeClient):
    """Register V1 and V2, create a producer pinned to V1, verify consumer receives schema_version=1."""
    topic = unique_topic("/default/pin_version_py")
    schema_client = client.schema()
    subject = unique_topic("version-pin-py")

    # Register V1
    schema_v1 = '{"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]}'
    await (
        schema_client.register_schema(subject)
        .with_type(SchemaType.JSON_SCHEMA)
        .with_schema_data(schema_v1.encode())
        .execute()
    )

    # Register V2
    schema_v2 = '{"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}, "required": ["id"]}'
    await (
        schema_client.register_schema(subject)
        .with_type(SchemaType.JSON_SCHEMA)
        .with_schema_data(schema_v2.encode())
        .execute()
    )

    # Producer pinned to V1
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_v1_pinned")
        .with_schema_version(subject, 1)
        .build()
    )
    await producer.create()

    # Consumer
    consumer = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name("consumer_version")
        .with_subscription("sub_version")
        .with_subscription_type(SubType.EXCLUSIVE)
        .build()
    )
    await consumer.subscribe()

    try:
        queue = await consumer.receive()
        await asyncio.sleep(0.2)

        await producer.send(b'{"id": 123}')

        msg = await receive_one(queue, timeout=5.0)
        assert msg.schema_version == 1, f"expected schema_version=1, got {msg.schema_version}"

        await consumer.ack(msg)
    finally:
        await producer.close()
        await consumer.close()


@pytest.mark.asyncio
async def test_producer_latest_version(client: DanubeClient):
    """Register V1 and V2, create a producer with latest schema, verify consumer receives schema_version=2."""
    topic = unique_topic("/default/latest_version_py")
    schema_client = client.schema()
    subject = unique_topic("latest-version-py")

    # Register V1
    schema_v1 = '{"type": "object", "properties": {"a": {"type": "string"}}}'
    await (
        schema_client.register_schema(subject)
        .with_type(SchemaType.JSON_SCHEMA)
        .with_schema_data(schema_v1.encode())
        .execute()
    )

    # Register V2
    schema_v2 = '{"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "integer"}}}'
    await (
        schema_client.register_schema(subject)
        .with_type(SchemaType.JSON_SCHEMA)
        .with_schema_data(schema_v2.encode())
        .execute()
    )

    # Producer without version pin (should use latest V2)
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_latest")
        .with_schema_subject(subject)
        .build()
    )
    await producer.create()

    # Consumer
    consumer = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name("consumer_latest")
        .with_subscription("sub_latest")
        .build()
    )
    await consumer.subscribe()

    try:
        queue = await consumer.receive()
        await asyncio.sleep(0.2)

        await producer.send(b'{"a": "test"}')

        msg = await receive_one(queue, timeout=5.0)
        assert msg.schema_version == 2, f"expected schema_version=2 (latest), got {msg.schema_version}"

        await consumer.ack(msg)
    finally:
        await producer.close()
        await consumer.close()
