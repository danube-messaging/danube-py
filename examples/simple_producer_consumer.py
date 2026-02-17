"""Simple example showing basic producer/consumer without schema validation.

This demonstrates raw byte message passing.
"""

import asyncio

from danube import DanubeClientBuilder, SubType


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/simple_topic"

    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name("simple_producer")
        .build()
    )
    await producer.create()
    print("Producer created")

    # Create consumer
    consumer = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name("simple_consumer")
        .with_subscription("simple_subscription")
        .with_subscription_type(SubType.EXCLUSIVE)
        .build()
    )
    await consumer.subscribe()
    print("Consumer subscribed")

    # Receive messages
    queue = await consumer.receive()
    count = 0

    print("\nListening for messages...\n")

    # Spawn producer task
    async def produce():
        for i in range(1, 6):
            message = f"Hello Danube! Message #{i}"
            message_id = await producer.send(message.encode())
            print(f"Sent: '{message}' (ID: {message_id})")
            await asyncio.sleep(1)

    producer_task = asyncio.create_task(produce())

    try:
        while count < 5:
            message = await queue.get()
            payload = message.payload.decode()
            print(f"Received: '{payload}' (ID: {message.msg_id})")

            # Acknowledge the message
            await consumer.ack(message)
            count += 1

        # Wait for producer to finish
        await producer_task

        print("\nDemo completed! Sent and received 5 messages")
    except asyncio.CancelledError:
        producer_task.cancel()
    finally:
        await producer.close()
        await consumer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted")
