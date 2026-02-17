"""Avro schema producer example.

Registers an Avro schema in the schema registry, then produces
JSON-serialized messages referencing that schema.
"""

import asyncio
import json
import time

from danube import DanubeClientBuilder, SchemaType


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/user_events"
    producer_name = "user_events_producer"

    # Register Avro schema
    avro_schema = json.dumps({
        "type": "record",
        "name": "UserEvent",
        "namespace": "com.example.events",
        "fields": [
            {"name": "user_id", "type": "string"},
            {"name": "action", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "metadata", "type": ["null", "string"], "default": None},
        ],
    })

    schema_client = client.schema()

    # Register the schema and get schema ID
    schema_id = await (
        schema_client.register_schema("user-events")
        .with_type(SchemaType.AVRO)
        .with_schema_data(avro_schema.encode())
        .execute()
    )
    print(f"Registered Avro schema with ID: {schema_id}")

    # Create producer with schema reference
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema_subject("user-events")
        .build()
    )
    await producer.create()
    print(f"Producer {producer_name} created with schema validation")

    # Send messages with Avro schema
    actions = ["login", "purchase", "logout", "view_product", "add_to_cart"]

    try:
        for i in range(100):
            event = {
                "user_id": f"user_{1000 + i}",
                "action": actions[i % len(actions)],
                "timestamp": int(time.time()),
                "metadata": f"session_id_{i}" if i % 2 == 0 else None,
            }

            # Serialize to JSON bytes
            avro_data = json.dumps(event).encode()

            try:
                message_id = await producer.send(avro_data)
                print(f"Sent event: {event['action']!r} with message ID: {message_id}")
            except Exception as e:
                print(f"Failed to send message: {e}")

            await asyncio.sleep(0.5)

        print("Sent 100 user events with Avro schema")
    except asyncio.CancelledError:
        pass
    finally:
        await producer.close()
        print("Producer closed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted")
