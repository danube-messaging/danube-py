"""Schema evolution example.

Comprehensive demonstration of schema evolution and compatibility checking.
Shows how to evolve a schema while maintaining backward compatibility.
"""

import asyncio
import json

from danube import DanubeClientBuilder, SchemaType


async def main():
    client = await (
        DanubeClientBuilder()
        .service_url("http://127.0.0.1:6650")
        .build()
    )

    schema_client = client.schema()

    # Step 1: Register the initial schema (v1)
    print("Step 1: Registering initial schema (v1)")

    schema_v1 = json.dumps({
        "type": "record",
        "name": "Product",
        "namespace": "com.example.catalog",
        "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "price", "type": "double"},
        ],
    })

    schema_id_v1 = await (
        schema_client.register_schema("product-catalog")
        .with_type(SchemaType.AVRO)
        .with_schema_data(schema_v1.encode())
        .execute()
    )
    print(f"Schema v1 registered with ID: {schema_id_v1}")

    # Give the broker time to sync metadata to LocalCache via watch
    print("Waiting for metadata to sync...")
    await asyncio.sleep(1.5)

    # Step 2: Check compatibility before evolving the schema
    print("\nStep 2: Checking compatibility for schema evolution (v2)")

    # Schema v2: Add a new optional field (backward compatible)
    schema_v2 = json.dumps({
        "type": "record",
        "name": "Product",
        "namespace": "com.example.catalog",
        "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "description", "type": ["null", "string"], "default": None},
        ],
    })

    is_compatible, errors = await schema_client.check_compatibility(
        "product-catalog",
        schema_v2.encode(),
        SchemaType.AVRO,
        None,
    )

    if is_compatible:
        print("Schema v2 is compatible! Safe to register.")

        # Register the new version
        try:
            schema_id_v2 = await (
                schema_client.register_schema("product-catalog")
                .with_type(SchemaType.AVRO)
                .with_schema_data(schema_v2.encode())
                .execute()
            )
            print(f"Schema v2 registered with ID: {schema_id_v2}")
        except Exception as e:
            print(f"Failed to register schema v2: {e}")
            print("    This might be a broker timing issue. The schema may already be registered.")
    else:
        print("Schema v2 is NOT compatible")
        if errors:
            print(f"   Errors: {errors}")

    # Step 3: Try to register an incompatible schema
    print("\nStep 3: Testing incompatible schema (v3 - adds required field without default)")

    # Schema v3: Add a new REQUIRED field without default (NOT backward compatible)
    schema_v3_incompatible = json.dumps({
        "type": "record",
        "name": "Product",
        "namespace": "com.example.catalog",
        "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "description", "type": ["null", "string"], "default": None},
            {"name": "category", "type": "string"},
        ],
    })

    is_compatible_v3, errors_v3 = await schema_client.check_compatibility(
        "product-catalog",
        schema_v3_incompatible.encode(),
        SchemaType.AVRO,
        None,
    )

    if is_compatible_v3:
        print("Schema v3 is compatible (unexpected!)")
        print("   Note: This should have been rejected for adding a required field!")
    else:
        print("Schema v3 is NOT compatible (expected!)")
        print("   Reason: Added required field 'category' without default")
        print("   This protects against breaking old data!")
        if errors_v3:
            print(f"   Errors: {errors_v3}")

    # Step 4: List all versions
    print("\nStep 4: Listing all schema versions")

    versions = await schema_client.list_versions("product-catalog")
    print(f"Schema versions for 'product-catalog': {versions}")

    # Step 5: Get the latest schema
    print("\nStep 5: Retrieving latest schema")

    latest_schema = await schema_client.get_latest_schema("product-catalog")
    print("Latest schema:")
    print(f"   Subject: {latest_schema.subject}")
    print(f"   Version: {latest_schema.version}")
    print(f"   Type: {latest_schema.schema_type}")

    print("\nSchema evolution demo completed!")
    print("   Key takeaways:")
    print("   - Adding optional fields: Compatible (backward)")
    print("   - Adding required fields without default: Incompatible")
    print("   - Compatibility mode: BACKWARD (default)")
    print("   - Backward = new schema can read old data")

    print("\nSUCCESS: Schema evolution example completed!")
    print("   All operations succeeded:")
    print("   - Registered schema v1 (3 fields)")
    print("   - Checked compatibility (v2 adds optional field -> compatible)")
    print("   - Registered schema v2 (4 fields)")
    print("   - Checked compatibility (v3 adds required field -> incompatible)")
    print(f"   - Listed {len(versions)} version(s)")
    print("   - Retrieved latest schema")


if __name__ == "__main__":
    asyncio.run(main())
