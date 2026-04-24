"""Key-Shared producer example.

Sends messages with routing keys so the broker dispatches them
to consumers based on consistent hashing of the key.
Pair with key_shared_consumer.py or key_shared_filtered_consumer.py.
"""

import asyncio

from danube import DanubeClientBuilder, DispatchStrategy


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/topic_key_shared"
    producer_name = "producer_key_shared"

    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_dispatch_strategy(DispatchStrategy.RELIABLE)
        .build()
    )

    await producer.create()
    print(f"The Producer {producer_name} was created")

    # Send messages with different routing keys.
    # All messages with the same key are guaranteed to be delivered
    # to the same consumer, in order.
    keys = ["payment", "shipping", "invoice", "payment", "shipping"]

    try:
        for i, key in enumerate(keys):
            payload = f"Order event #{i} for key={key}"
            message_id = await producer.send_with_key(
                payload.encode(), None, key
            )
            print(f"Sent message id={message_id} key={key} payload={payload}")
            await asyncio.sleep(0.5)
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
