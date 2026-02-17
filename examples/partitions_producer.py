"""Partitioned topic producer example.

Creates a producer with 3 partitions for horizontal scaling.
Pair with partitions_consumer.py.
"""

import asyncio

from danube import DanubeClientBuilder


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/partitioned_topic"
    producer_name = "prod_part"

    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_partitions(3)
        .build()
    )

    await producer.create()
    print(f"The Producer {producer_name} was created")

    try:
        for i in range(100):
            encoded_data = f"Hello Danube {i}".encode()

            message_id = await producer.send(encoded_data)
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
