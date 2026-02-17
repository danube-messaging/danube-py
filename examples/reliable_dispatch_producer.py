"""Reliable dispatch producer example.

Creates a producer with reliable dispatch (WAL-backed delivery).
Generates dynamic messages with varied phrases.
Pair with reliable_dispatch_consumer.py.
"""

import asyncio

from danube import DanubeClientBuilder, DispatchStrategy


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/reliable_topic"
    producer_name = "prod_json_reliable"

    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_dispatch_strategy(DispatchStrategy.RELIABLE)
        .build()
    )

    await producer.create()
    print(f"The Producer {producer_name} was created")

    # Define word variations for dynamic message generation
    subjects = [
        "The Danube system",
        "Danube platform",
        "Danube messaging service",
        "The Danube application",
    ]
    verbs = ["processes", "handles", "manages", "delivers"]
    objects = [
        "messages efficiently",
        "data reliably",
        "requests quickly",
        "events seamlessly",
    ]
    conclusions = [
        "with high performance.",
        "at scale.",
        "with real-time.",
        "without issues.",
    ]

    try:
        for i in range(100):
            # Generate a dynamic message with 3-4 phrases
            message = (
                f"{subjects[i % len(subjects)]} "
                f"{verbs[i % len(verbs)]} "
                f"{objects[i % len(objects)]}, "
                f"with low latency and high throughput. "
                f"It is designed for reliability, "
                f"{conclusions[i % len(conclusions)]}"
            )

            try:
                message_id = await producer.send(message.encode())
                print(f"The Message with id {message_id} was sent")
            except Exception as e:
                print(f"Failed to send message: {e}")

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
