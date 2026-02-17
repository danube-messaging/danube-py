# Danube Python Client Examples

This directory contains examples demonstrating various features of the Danube messaging platform using the Python client library.

## Table of Contents

1. [Basic Examples](#basic-examples)
2. [Schema Registry Examples](#schema-registry-examples)
3. [Advanced Features](#advanced-features)
4. [Running Examples](#running-examples)

---

## Basic Examples

### 1. **simple_producer_consumer.py**
**Purpose**: Demonstrates the simplest way to send and receive raw byte messages without schema validation.

**Key Features**:
- Basic producer/consumer setup
- Raw byte message passing
- Message acknowledgment
- Single producer-consumer pair

**Use Case**: Quick prototyping, simple message passing, when schema validation is not needed.

```bash
python examples/simple_producer_consumer.py
```

---

### 2. **partitions_producer.py** & **partitions_consumer.py**
**Purpose**: Shows how to use topic partitioning for horizontal scaling and parallel processing.

**Key Features**:
- Creating partitioned topics
- Automatic message distribution across partitions
- Parallel message consumption

**Use Case**: High-throughput scenarios where messages can be processed independently in parallel.

```bash
# Terminal 1 - Start consumer
python examples/partitions_consumer.py

# Terminal 2 - Start producer
python examples/partitions_producer.py
```

---

### 3. **reliable_dispatch_producer.py** & **reliable_dispatch_consumer.py**
**Purpose**: Demonstrates reliable message delivery with acknowledgments and retry mechanisms.

**Key Features**:
- Reliable dispatch mode for guaranteed delivery
- Automatic retry on failures
- Message acknowledgment tracking

**Use Case**: Critical messages that must be delivered (e.g., payment notifications, order processing).

```bash
# Terminal 1 - Start consumer
python examples/reliable_dispatch_consumer.py

# Terminal 2 - Start producer
python examples/reliable_dispatch_producer.py
```

---

## Schema Registry Examples

### 4. **json_producer.py** & **json_consumer.py**
**Purpose**: Shows how to use JSON Schema for message validation with typed data structures.

**Key Features**:
- JSON Schema registration in Schema Registry
- Automatic serialization/deserialization
- Type-safe message passing
- Schema validation on both producer and consumer sides

**Use Case**: Applications using JSON for structured data with schema evolution needs.

```bash
# Terminal 1 - Start consumer
python examples/json_consumer.py

# Terminal 2 - Start producer
python examples/json_producer.py
```

**Schema Type**: `json_schema`

---

### 5. **json_consumer_validated.py**
**Purpose**: Demonstrates consumer-side schema validation against the Schema Registry at startup.

**Key Features**:
- Fetches schema from registry before consuming
- Validates a sample dict against JSON Schema definition
- Fails at startup if sample doesn't match schema
- Prevents runtime deserialization errors
- Schema version tracking and logging

**Use Case**: Production consumers that need to ensure their data definitions match the producer's schema, preventing silent data loss or deserialization failures.

```bash
# Run the producer first to register schema
python examples/json_producer.py

# Then run the validated consumer
python examples/json_consumer_validated.py
```

**What it validates**:
- Field names match schema properties
- Field types are compatible (string, integer, etc.)
- Required fields are present

**Dependencies**: Requires `jsonschema` package:
```bash
pip install jsonschema
```

---

### 6. **avro_producer.py** & **avro_consumer.py**
**Purpose**: Demonstrates Apache Avro schema usage for structured event serialization.

**Key Features**:
- Avro schema registration
- Structured event serialization
- Schema evolution support
- Strongly-typed data structures

**Use Case**: High-performance applications requiring efficient serialization and schema evolution.

```bash
# Terminal 1 - Start consumer
python examples/avro_consumer.py

# Terminal 2 - Start producer
python examples/avro_producer.py
```

**Schema Type**: `avro`

**Example Schema**:
```json
{
    "type": "record",
    "name": "UserEvent",
    "namespace": "com.example.events",
    "fields": [
        {"name": "user_id", "type": "string"},
        {"name": "action", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "metadata", "type": ["null", "string"], "default": null}
    ]
}
```

---

### 7. **schema_evolution.py**
**Purpose**: Comprehensive demonstration of schema evolution and compatibility checking.

**Key Features**:
- Schema version management
- Compatibility checking (backward/forward/full)
- Safe schema evolution
- Listing all schema versions
- Retrieving latest schema

**Use Case**: Understanding how to evolve data schemas safely over time without breaking existing consumers.

```bash
python examples/schema_evolution.py
```

**Demonstrates**:
- **Compatible change**: Adding optional fields with defaults
- **Incompatible change**: Adding required fields without defaults
- Schema version history tracking
- Compatibility mode enforcement

---

## Advanced Features

### Topic Partitioning
Partitions allow horizontal scaling by distributing messages across multiple partitions:

```python
producer = (
    client.new_producer()
    .with_topic("/default/my_topic")
    .with_name("my_producer")
    .with_partitions(3)  # Create 3 partitions
    .build()
)
```

### Reliable Dispatch
Ensures message delivery with automatic retries:

```python
from danube import DispatchStrategy

producer = (
    client.new_producer()
    .with_topic("/default/my_topic")
    .with_name("my_producer")
    .with_dispatch_strategy(DispatchStrategy.RELIABLE)
    .build()
)
```

### Schema Validation
Register schemas and enable validation:

```python
from danube import SchemaType, CompatibilityMode

# 1. Get schema client from DanubeClient
schema_client = client.schema()

# 2. Register schema
schema_id = await (
    schema_client.register_schema("my-subject")
    .with_type(SchemaType.AVRO)
    .with_schema_data(schema_bytes)
    .execute()
)

# 3. Check compatibility before evolution
compat_result = await schema_client.check_compatibility(
    "my-subject",
    new_schema_bytes,
    SchemaType.AVRO,
    None,  # Use subject's default mode
)

# 4. Set compatibility mode for a subject
await schema_client.set_compatibility_mode("critical-subject", CompatibilityMode.FULL)

# 5. Create producer with schema subject
producer = (
    client.new_producer()
    .with_topic("/default/my_topic")
    .with_name("my_producer")
    .with_schema_subject("my-subject")
    .build()
)
```

---

## Supported Schema Types

| Type | Description | Use Case |
|------|-------------|----------|
| `SchemaType.BYTES` | Raw binary data (no validation) | Simple messaging, custom formats |
| `SchemaType.STRING` | UTF-8 text data | Plain text messages |
| `SchemaType.NUMBER` | Numeric data (int, float, double) | Simple numeric values |
| `SchemaType.JSON_SCHEMA` | JSON with schema validation | Structured JSON data |
| `SchemaType.AVRO` | Apache Avro binary format | High-performance, schema evolution |
| `SchemaType.PROTOBUF` | Protocol Buffers | Cross-language compatibility |

---

## Compatibility Modes

Schema evolution is controlled by compatibility modes. Set these per-subject to define evolution rules:

| Mode | Description | Allows | Use Case |
|------|-------------|--------|----------|
| `CompatibilityMode.BACKWARD` | New schema reads old data | Add optional fields, remove fields | **Default**. Consumers upgrade before producers |
| `CompatibilityMode.FORWARD` | Old schema reads new data | Add required fields, remove optional fields | Producers upgrade before consumers |
| `CompatibilityMode.FULL` | Both directions | Only safe changes (add optional) | **Strictest**. Critical schemas |
| `CompatibilityMode.NONE` | No validation | Any change | Development/testing only |

---

## Running Examples

### Prerequisites

1. **Start the Danube broker** (from the `docker/` directory):
   ```bash
   cd docker/
   docker compose up -d
   ```

2. **Install the danube-client** package:
   ```bash
   pip install -e .
   ```

3. **For the validated consumer example**, install `jsonschema`:
   ```bash
   pip install jsonschema
   ```

### Running Individual Examples

```bash
# Basic examples
python examples/simple_producer_consumer.py
python examples/partitions_producer.py
python examples/partitions_consumer.py

# Schema registry examples
python examples/json_producer.py
python examples/json_consumer.py
python examples/json_consumer_validated.py  # Requires jsonschema
python examples/avro_producer.py
python examples/avro_consumer.py
python examples/schema_evolution.py

# Reliable dispatch
python examples/reliable_dispatch_producer.py
python examples/reliable_dispatch_consumer.py
```

### Recommended Learning Path

1. **Start simple**: `simple_producer_consumer.py` — understand basic messaging
2. **Add schemas**: `json_producer.py` + `json_consumer.py` — learn schema registration
3. **Validate schemas**: `json_consumer_validated.py` — production-ready validation
4. **Schema evolution**: `schema_evolution.py` — understand compatibility rules
5. **High performance**: `avro_producer.py` + `avro_consumer.py` — Avro serialization
6. **Scale up**: `partitions_producer.py` + `partitions_consumer.py` — horizontal scaling
7. **Reliability**: `reliable_dispatch_*.py` examples — guaranteed delivery
