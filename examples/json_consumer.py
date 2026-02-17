"""JSON Schema consumer example.

Consumes JSON-encoded messages and deserializes them into Python dicts.
Pair with json_producer.py.
"""

import asyncio
import json

from danube import DanubeClientBuilder, SubType


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/test_topic"
    consumer_name = "cons_json"
    subscription_name = "subs_json"

    consumer = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name(consumer_name)
        .with_subscription(subscription_name)
        .with_subscription_type(SubType.EXCLUSIVE)
        .build()
    )

    # Subscribe to the topic
    await consumer.subscribe()
    print(f"The Consumer {consumer_name} was created")

    # Start receiving messages
    queue = await consumer.receive()

    try:
        while True:
            message = await queue.get()
            try:
                decoded = json.loads(message.payload)
                print(f"Received message: {decoded}")

                # Acknowledge the message
                await consumer.ack(message)
            except json.JSONDecodeError as e:
                print(f"Failed to deserialize message: {e}")
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
