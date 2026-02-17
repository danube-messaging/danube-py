"""Example: basic consumer that receives messages from a Danube topic."""

import asyncio

from danube import DanubeClientBuilder, SubType


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    consumer = (
        client.new_consumer()
        .with_topic("/default/test-topic")
        .with_consumer_name("test-consumer")
        .with_subscription("test-subscription")
        .with_subscription_type(SubType.SHARED)
        .build()
    )

    await consumer.subscribe()
    print("Consumer subscribed successfully")

    queue = await consumer.receive()

    try:
        while True:
            message = await queue.get()
            data = message.payload.decode("utf-8")
            print(f"Received: {data}")
            await consumer.ack(message)
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.close()
        print("Consumer closed")


if __name__ == "__main__":
    asyncio.run(main())
