#!/usr/bin/env python3
"""
Example usage of Gravitino namespace implementation for Lance.

This example demonstrates how to:
1. Initialize a Gravitino namespace
2. Create catalogs and schemas
3. Declare and manage Lance tables
4. List and describe namespaces and tables

Prerequisites:
- Apache Gravitino server running with Lance REST service enabled

To run this example:
    python gravitino_example.py
"""

import os
import sys
from lance_namespace_impls import GravitinoNamespace
from lance_namespace_urllib3_client.models import (
    CreateNamespaceRequest,
    DeclareTableRequest,
    DeregisterTableRequest,
    DescribeNamespaceRequest,
    DescribeTableRequest,
    DropNamespaceRequest,
    ListNamespacesRequest,
    ListTablesRequest,
)


def main():
    # Configuration - adjust these values for your Gravitino setup
    gravitino_endpoint = os.environ.get("GRAVITINO_ENDPOINT", "http://localhost:9101")
    auth_token = os.environ.get("GRAVITINO_AUTH_TOKEN")  # Optional

    print(f"Connecting to Gravitino at {gravitino_endpoint}")

    # Initialize the Gravitino namespace
    config = {
        "endpoint": gravitino_endpoint,
    }
    if auth_token:
        config["auth_token"] = auth_token

    try:
        namespace = GravitinoNamespace(**config)
        print(f"✓ Connected to {namespace.namespace_id()}")
    except Exception as e:
        print(f"✗ Failed to connect to Gravitino: {e}")
        sys.exit(1)

    # Example catalog and schema names
    catalog_name = "lance_catalog111"
    schema_name = "example_schema"
    table_name = "my_lance_table"

    try:
        # 1. List existing catalogs
        print("\n1. Listing existing catalogs...")
        list_catalogs_request = ListNamespacesRequest(id=[])
        catalogs_response = namespace.list_namespaces(list_catalogs_request)
        print(f"Existing catalogs: {list(catalogs_response.namespaces)}")

        # 2. Create a catalog
        print(f"\n2. Creating catalog '{catalog_name}'...")
        create_catalog_request = CreateNamespaceRequest(
            id=[catalog_name],
            properties={"description": "Example Lance catalog"}
        )
        try:
            namespace.create_namespace(create_catalog_request)
            print(f"Created catalog '{catalog_name}'")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"Catalog '{catalog_name}' already exists")
            else:
                raise

        # 3. Create a schema in the catalog
        print(f"\n3. Creating schema '{schema_name}' in catalog '{catalog_name}'...")
        create_schema_request = CreateNamespaceRequest(
            id=[catalog_name, schema_name],
            properties={"description": "Example Lance schema"}
        )
        try:
            namespace.create_namespace(create_schema_request)
            print(f"Created schema '{catalog_name}.{schema_name}'")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"Schema '{catalog_name}.{schema_name}' already exists")
            else:
                raise

        # 4. List schemas in the catalog
        print(f"\n4. Listing schemas in catalog '{catalog_name}'...")
        list_schemas_request = ListNamespacesRequest(id=[catalog_name])
        schemas_response = namespace.list_namespaces(list_schemas_request)
        print(f"   Schemas: {list(schemas_response.namespaces)}")

        # 5. Describe the schema
        print(f"\n5. Describing schema '{catalog_name}.{schema_name}'...")
        describe_schema_request = DescribeNamespaceRequest(id=[catalog_name, schema_name])
        schema_description = namespace.describe_namespace(describe_schema_request)
        print(f"   Schema properties: {schema_description.properties}")

        # 6. Declare a Lance table
        print(f"\n6. Declaring Lance table '{table_name}'...")
        table_location = f"/tmp/lance_data/{catalog_name}/{schema_name}/{table_name}"
        declare_table_request = DeclareTableRequest(
            id=[catalog_name, schema_name, table_name],
            location=table_location
        )
        try:
            table_response = namespace.declare_table(declare_table_request)
            print(f"   Declared table '{catalog_name}.{schema_name}.{table_name}'")
            print(f"   Table location: {table_response.location}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"   Table '{catalog_name}.{schema_name}.{table_name}' already exists")
            else:
                raise

        # 7. List tables in the schema
        print(f"\n7. Listing tables in schema '{catalog_name}.{schema_name}'...")
        list_tables_request = ListTablesRequest(id=[catalog_name, schema_name])
        tables_response = namespace.list_tables(list_tables_request)
        print(f"   Tables: {list(tables_response.tables)}")

        # 8. Describe the table
        print(f"\n8. Describing table '{catalog_name}.{schema_name}.{table_name}'...")
        describe_table_request = DescribeTableRequest(
            id=[catalog_name, schema_name, table_name]
        )
        try:
            table_description = namespace.describe_table(describe_table_request)
            print(f"   Table location: {table_description.location}")
            print(f"   Storage options: {table_description.storage_options}")
        except Exception as e:
            print(f"   ⚠ Could not describe table: {e}")

        # 9. Clean up (optional) - uncomment to remove created resources
        print(f"\n9. Cleanup (skipped - uncomment to enable)...")
        print(f"   Deregistering table '{table_name}'...")
        deregister_request = DeregisterTableRequest(
            id=[catalog_name, schema_name, table_name]
        )
        namespace.deregister_table(deregister_request)
        print(f"   Deregistered table")
        
        print(f"   Dropping schema '{schema_name}'...")
        drop_schema_request = DropNamespaceRequest(id=[catalog_name, schema_name])
        namespace.drop_namespace(drop_schema_request)
        print(f"   Dropped schema")
        
        print(f"   Dropping catalog '{catalog_name}'...")
        drop_catalog_request = DropNamespaceRequest(id=[catalog_name], behavior="CASCADE")
        namespace.drop_namespace(drop_catalog_request)
        print(f"   Dropped catalog")

        print("\n Example completed successfully!")
        print("\nNext steps:")
        print("- Use Lance to create and query tables in the declared locations")
        print("- Explore the Gravitino web UI to see the created catalogs and schemas")
        print("- Check the Gravitino documentation for advanced configuration options")

    except Exception as e:
        print(f"\n✗ Error during example execution: {e}")
        sys.exit(1)

    finally:
        # Close the namespace connection
        namespace.close()


if __name__ == "__main__":
    main()