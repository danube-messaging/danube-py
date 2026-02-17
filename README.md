# Danube-py client

The Python async client library for interacting with Danube Messaging Broker platform.

[Danube](https://github.com/danube-messaging/danube) is an open-source **distributed** Messaging platform written in Rust. Consult [the documentation](https://danube-docs.dev-state.com/) for supported concepts and the platform architecture.

## Features

### 📤 Producer Capabilities

- **Basic Messaging** - Send messages with byte payloads
- **Partitioned Topics** - Distribute messages across multiple partitions for horizontal scaling
- **Reliable Dispatch** - Guaranteed message delivery with persistence (WAL + cloud storage)
- **Schema Integration** - Type-safe messaging with automatic validation (Bytes, String, Number, Avro, JSON Schema, Protobuf)

### 📥 Consumer Capabilities

- **Flexible Subscriptions** - Three subscription types for different use cases:
  - **Exclusive** - Single active consumer, guaranteed ordering
  - **Shared** - Load balancing across multiple consumers, parallel processing
  - **Failover** - High availability with automatic standby promotion
- **Message Acknowledgment** - Reliable message processing with at-least-once delivery
- **Partitioned Consumption** - Automatic handling of messages from all partitions

### 🔐 Schema Registry

- **Schema Management** - Register, version, and retrieve schemas (JSON Schema, Avro, Protobuf)
- **Compatibility Checking** - Validate schema evolution (Backward, Forward, Full, None modes)
- **Type Safety** - Automatic validation against registered schemas
- **Schema Evolution** - Safe schema updates with compatibility enforcement

### 🏗️ Client Features

- **Async/Await** - Built on `asyncio` and `grpc.aio` for efficient async I/O
- **Connection Pooling** - Shared connection management across producers/consumers
- **Automatic Reconnection** - Resilient connection handling with retry logic
- **Topic Namespaces** - Organize topics with namespace structure (`/namespace/topic-name`)

## Installation

```bash
pip install danube-client
```

Or install from source:

```bash
git clone https://github.com/danube-messaging/danube-py.git
cd danube-py
pip install -e .
```

## Example Usage

Check out the [example files](https://github.com/danube-messaging/danube-py/tree/main/examples).

### Start the Danube server

Use the [instructions from the documentation](https://danube-docs.dev-state.com/) to run the Danube broker/cluster.

### Create Producer

```python
import asyncio
from danube import DanubeClientBuilder

async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/test_topic"
    producer_name = "test_producer"

    producer = (
        client.new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .build()
    )

    await producer.create()
    print(f"The Producer {producer_name} was created")

    payload = b"Hello Danube"
    message_id = await producer.send(payload)
    print(f"The Message with id {message_id} was sent")

    await producer.close()

asyncio.run(main())
```

### Reliable Dispatch (optional)

Reliable dispatch can be enabled when creating the producer, the broker will stream the messages to the consumer from WAL and cloud storage.

```python
from danube import DispatchStrategy

producer = (
    client.new_producer()
    .with_topic(topic)
    .with_name(producer_name)
    .with_dispatch_strategy(DispatchStrategy.RELIABLE)
    .build()
)
```

### Create Consumer

```python
import asyncio
from danube import DanubeClientBuilder, SubType

async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    topic = "/default/test_topic"
    consumer_name = "test_consumer"
    subscription_name = "test_subscription"

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

    while True:
        message = await queue.get()
        payload = message.payload.decode()
        print(f"Received message: {payload!r}")

        # Acknowledge the message
        await consumer.ack(message)

asyncio.run(main())
```

### Schema Registry

```python
import json
from danube import SchemaType

schema_client = client.schema()

# Register a JSON schema
json_schema = json.dumps({
    "type": "object",
    "properties": {
        "field1": {"type": "string"},
        "field2": {"type": "integer"},
    },
})

schema_id = await (
    schema_client.register_schema("my-app-events")
    .with_type(SchemaType.JSON_SCHEMA)
    .with_schema_data(json_schema.encode())
    .execute()
)

# Create producer with schema reference
producer = (
    client.new_producer()
    .with_topic("/default/test_topic")
    .with_name("schema_producer")
    .with_schema_subject("my-app-events")
    .build()
)
```

Browse the [examples directory](https://github.com/danube-messaging/danube-py/tree/main/examples) for complete working code.

## Contribution

Working on improving and adding new features. Please feel free to contribute or report any issues you encounter.

### Running Integration Tests

Before submitting a PR, start the test cluster and run the integration tests:

```bash
# 1. Start the cluster
cd docker/
docker compose up -d

# 2. Wait for the broker to be healthy
docker compose ps

# 3. Run the integration tests from the repository root
cd ..
pytest integration_tests/ -v --timeout=120

# 4. Stop the cluster when done
cd docker/
docker compose down -v
```

### Regenerating gRPC stubs

Make sure the proto files are the latest from the [Danube project](https://github.com/danube-messaging/danube/tree/main/danube-core/proto).

Install the required tools:

```bash
pip install grpcio-tools
```

Generate the Python gRPC code from the proto files:

```bash
python -m grpc_tools.protoc \
    --proto_path=danube/proto \
    --python_out=danube/proto \
    --grpc_python_out=danube/proto \
    danube/proto/DanubeApi.proto

python -m grpc_tools.protoc \
    --proto_path=danube/proto \
    --python_out=danube/proto \
    --grpc_python_out=danube/proto \
    danube/proto/SchemaRegistry.proto
```

Then fix the imports in the generated `*_grpc.py` files to use package-relative imports:

```bash
sed -i 's/^import DanubeApi_pb2/from danube.proto import DanubeApi_pb2/' danube/proto/DanubeApi_pb2_grpc.py
sed -i 's/^import SchemaRegistry_pb2/from danube.proto import SchemaRegistry_pb2/' danube/proto/SchemaRegistry_pb2_grpc.py
```
