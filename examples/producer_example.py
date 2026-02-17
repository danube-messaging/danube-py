"""Example: basic producer that sends messages to a Danube topic."""

import asyncio
import json

from danube import DanubeClientBuilder


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    producer = (
        client.new_producer()
        .with_topic("/default/test-topic")
        .with_name("test-producer")
        .build()
    )

    await producer.create()
    print("Producer created successfully")

    for i in range(10):
        data = json.dumps({"message": f"hello {i}"}).encode()
        seq_id = await producer.send(data)
        print(f"Sent message {i}, sequence_id={seq_id}")

    print("Done sending messages")


if __name__ == "__main__":
    asyncio.run(main())
