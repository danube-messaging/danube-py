"""Reliable dispatch consumer example.

Consumes messages from a reliable dispatch topic, tracking total bytes received.
Pair with reliable_dispatch_producer.py.
"""

import asyncio

from danube import DanubeClientBuilder, SubType


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/reliable_topic"
    consumer_name = "cons_reliable"
    subscription_name = "subs_reliable"

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
    total_received_size = 0

    try:
        while True:
            message = await queue.get()
            payload = message.payload
            total_received_size += len(payload)

            try:
                message_str = payload.decode()
                print(
                    f"Received message: {message_str} | "
                    f"offset: {message.msg_id.topic_offset} | "
                    f"total received bytes: {total_received_size}"
                )
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
