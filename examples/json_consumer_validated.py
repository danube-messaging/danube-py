"""Validated JSON consumer example.

Demonstrates consumer-side schema validation against the Schema Registry at startup.
Fetches the schema, validates a sample dict against it using jsonschema, then
proceeds to consume messages with confidence.

Requires: pip install jsonschema

Pair with json_producer.py (run the producer first to register the schema).
"""

import asyncio
import json

from danube import DanubeClientBuilder, SubType

try:
    import jsonschema
except ImportError:
    raise SystemExit(
        "This example requires the 'jsonschema' package.\n"
        "Install it with: pip install jsonschema"
    )


async def validate_dict_against_registry(schema_client, subject: str, sample: dict) -> int:
    """Validates that a sample dict matches the JSON Schema from the registry.

    Returns the schema version number if validation succeeds.
    Raises on validation failure — preventing the consumer from starting.
    """
    print(f"Fetching schema from registry for subject: {subject}")

    schema_response = await schema_client.get_latest_schema(subject)

    print(f"Retrieved schema version: {schema_response.version}")
    print(f"Schema type: {schema_response.schema_type}")

    schema_def = json.loads(schema_response.schema_definition)

    print("Compiling JSON Schema validator...")
    validator = jsonschema.Draft7Validator(schema_def)

    print("Validating sample against schema...")
    errors = list(validator.iter_errors(sample))

    if errors:
        print(f"\nVALIDATION FAILED: Sample does not match schema v{schema_response.version}")
        print("   The consumer data definition is incompatible with the registered schema.")
        print("\n   Validation errors:")
        for error in errors:
            print(f"   - {error.message}")
        print("\n   Fix: Update the sample dict to match the schema in the registry.")
        raise RuntimeError("Sample validation failed - consumer cannot start")

    print(f"Sample validated successfully against schema v{schema_response.version}")
    return schema_response.version


async def main():
    print("Starting validated JSON consumer example\n")

    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/test_topic"
    consumer_name = "cons_json_validated"
    subscription_name = "subs_json_validated"

    # Step 1: Validate sample against registry schema
    print("Step 1: Validating consumer sample against schema registry\n")

    schema_client = client.schema()

    # This will fail at startup if the sample doesn't match the schema
    schema_version = await validate_dict_against_registry(
        schema_client,
        "my-app-events",
        {"field1": "validation_test", "field2": 0},
    )

    print(f"\nConsumer validated against schema version: {schema_version}")
    print("   Safe to proceed with typed deserialization\n")

    # Step 2: Create and subscribe consumer
    print("Step 2: Creating consumer and subscribing to topic\n")

    consumer = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name(consumer_name)
        .with_subscription(subscription_name)
        .with_subscription_type(SubType.EXCLUSIVE)
        .build()
    )

    await consumer.subscribe()
    print(f"Consumer {consumer_name} subscribed to {topic}")

    # Step 3: Receive and deserialize messages
    print(f"\nStep 3: Listening for messages...\n")

    queue = await consumer.receive()

    try:
        while True:
            message = await queue.get()
            try:
                decoded = json.loads(message.payload)
                print(f"Received valid message:")
                print(f"   field1: {decoded.get('field1')}")
                print(f"   field2: {decoded.get('field2')}")
                print(f"   Message ID: {message.msg_id}")
                print()

                # Acknowledge the message
                await consumer.ack(message)
            except json.JSONDecodeError as e:
                print(f"Deserialization failed: {e}")
                print(f"   This might indicate schema drift - check schema version {schema_version}")
                print("   Message will NOT be acknowledged (will retry or go to DLQ)")
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.close()
        print("Consumer closed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted")
