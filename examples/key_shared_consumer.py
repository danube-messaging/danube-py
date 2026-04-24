"""Key-Shared consumer example.

Subscribes with KEY_SHARED type — the broker routes messages so that
all messages with the same routing key go to the same consumer.
Pair with key_shared_producer.py.
"""

import asyncio

from danube import DanubeClientBuilder, SubType


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/topic_key_shared"
    consumer_name = "consumer_key_shared"
    subscription_name = "sub_key_shared"

    consumer = (
        client.new_consumer()
        .with_topic(topic)
        .with_consumer_name(consumer_name)
        .with_subscription(subscription_name)
        .with_subscription_type(SubType.KEY_SHARED)
        .build()
    )

    await consumer.subscribe()
    print(f"The Consumer {consumer_name} was created")

    queue = await consumer.receive()

    try:
        while True:
            message = await queue.get()
            routing_key = message.routing_key if message.HasField("routing_key") else ""
            print(
                f"Received: key={routing_key} "
                f"payload={message.payload.decode()} "
                f"topic={message.msg_id.topic_name} "
                f"offset={message.msg_id.topic_offset}"
            )
            await consumer.ack(message)
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
