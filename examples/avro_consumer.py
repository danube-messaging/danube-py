"""Avro schema consumer example.

Consumes messages produced with an Avro schema and deserializes them.
Pair with avro_producer.py.
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

    topic = "/default/user_events"
    consumer_name = "user_events_consumer"
    subscription_name = "user_events_subscription"

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
    print(f"Consumer {consumer_name} subscribed to {topic}")

    # Start receiving messages
    queue = await consumer.receive()

    print("Listening for user events (Avro schema)...\n")

    try:
        while True:
            message = await queue.get()
            try:
                event = json.loads(message.payload)
                print("Received User Event:")
                print(f"   User ID: {event['user_id']}")
                print(f"   Action: {event['action']}")
                print(f"   Timestamp: {event['timestamp']}")
                if event.get("metadata"):
                    print(f"   Metadata: {event['metadata']}")
                print(f"   Message ID: {message.msg_id}")
                print()

                # Acknowledge the message
                await consumer.ack(message)
            except (json.JSONDecodeError, KeyError) as e:
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
