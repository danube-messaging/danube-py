"""Partitioned topic consumer example.

Consumes messages from a partitioned topic.
Pair with partitions_producer.py.
"""

import asyncio

from danube import DanubeClientBuilder, SubType


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/partitioned_topic"
    consumer_name = "cons_part"
    subscription_name = "subs_part"

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
                message_str = message.payload.decode()
                print(f"Received message: {message_str!r}")
                await consumer.ack(message)
            except UnicodeDecodeError as e:
                print(f"Failed to convert Payload to String: {e}")
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
