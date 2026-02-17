"""JSON Schema producer example.

Registers a JSON Schema in the schema registry, then produces
JSON-encoded messages referencing that schema.
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

    topic = "/default/test_topic"
    producer_name = "prod_json"

    # Register schema in schema registry
    json_schema = json.dumps({
        "type": "object",
        "properties": {
            "field1": {"type": "string"},
            "field2": {"type": "integer"},
        },
    })

    schema_client = client.schema()
    schema_id = await (
        schema_client.register_schema("my-app-events")
        .with_type(SchemaType.JSON_SCHEMA)
        .with_schema_data(json_schema.encode())
        .execute()
    )
    print(f"Registered schema with ID: {schema_id}")

    # Create producer with schema reference
    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema_subject("my-app-events")
        .build()
    )
    await producer.create()
    print(f"The Producer {producer_name} was created")

    try:
        for i in range(100):
            message = {"field1": f"value{i}", "field2": 2020 + i}
            json_bytes = json.dumps(message).encode()

            message_id = await producer.send(json_bytes)
            print(f"The Message with id {message_id} was sent")

            await asyncio.sleep(1)
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
