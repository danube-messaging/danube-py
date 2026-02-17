"""Schema subject tests: registration, producer with schema, version evolution."""

from __future__ import annotations

import pytest

from danube import DanubeClient, SchemaType
from integration_tests.conftest import unique_topic


@pytest.mark.asyncio
async def test_producer_with_registered_schema(client: DanubeClient):
    """Register a JSON schema, create a producer referencing that subject, and send a message."""
    topic = unique_topic("/default/schema_registered_py")
    schema_client = client.schema()

    json_schema = '{"type": "object", "properties": {"msg": {"type": "string"}}}'

    await (
        schema_client.register_schema("test-registered-schema-py")
        .with_type(SchemaType.JSON_SCHEMA)
        .with_schema_data(json_schema.encode())
        .execute()
    )

    # Create producer with schema reference
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_registered")
        .with_schema_subject("test-registered-schema-py")
        .build()
    )
    await producer.create()

    try:
        payload = '{"msg": "hello"}'
        await producer.send(payload.encode())
    finally:
        await producer.close()


@pytest.mark.asyncio
async def test_producer_without_schema(client: DanubeClient):
    """Producer without a schema can send arbitrary bytes (implicit Bytes schema)."""
    topic = unique_topic("/default/no_schema_py")

    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("producer_no_schema")
        .build()
    )
    await producer.create()
    try:
        await producer.send(b"any bytes work")
    finally:
        await producer.close()


@pytest.mark.asyncio
async def test_schema_version_evolution(client: DanubeClient):
    """Register V1 and V2 of a schema, verify latest version is V2."""
    schema_client = client.schema()
    subject = unique_topic("versioned-schema-py")

    # V1
    schema_v1 = '{"type": "object", "properties": {"name": {"type": "string"}}}'
    id_v1 = await (
        schema_client.register_schema(subject)
        .with_type(SchemaType.JSON_SCHEMA)
        .with_schema_data(schema_v1.encode())
        .execute()
    )

    # V2 (add optional field)
    schema_v2 = '{"type": "object", "properties": {"name": {"type": "string"}, "email": {"type": "string"}}}'
    id_v2 = await (
        schema_client.register_schema(subject)
        .with_type(SchemaType.JSON_SCHEMA)
        .with_schema_data(schema_v2.encode())
        .execute()
    )

    # Same subject -> same schema_id
    assert id_v1 == id_v2, f"expected same schema_id for same subject, got V1={id_v1} V2={id_v2}"

    # Latest should be V2
    latest = await schema_client.get_latest_schema(subject)
    assert latest.version == 2, f"expected latest version 2, got {latest.version}"
