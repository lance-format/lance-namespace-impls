"""
Integration tests for Iceberg REST Catalog namespace implementation.

This test uses Lakekeeper as the Iceberg REST Catalog implementation.
To run these tests, start the catalog with:
  cd docker/lakekeeper && docker-compose up -d

Tests are automatically skipped if the catalog is not available.
"""

import os
import uuid
import urllib.request
import urllib.error
import unittest

import pytest

from lance_namespace_impls.iceberg import IcebergNamespace
from lance_namespace_impls.rest_client import (
    NamespaceNotFoundException,
    TableNotFoundException,
)
from lance_namespace_urllib3_client.models import (
    CreateEmptyTableRequest,
    CreateNamespaceRequest,
    DeregisterTableRequest,
    DescribeNamespaceRequest,
    DescribeTableRequest,
    DropNamespaceRequest,
    ListNamespacesRequest,
    ListTablesRequest,
    NamespaceExistsRequest,
    TableExistsRequest,
)


ICEBERG_ENDPOINT = os.environ.get("ICEBERG_ENDPOINT", "http://localhost:8282/catalog")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "test_warehouse")


def check_iceberg_available():
    """Check if Iceberg REST Catalog is available."""
    try:
        url = f"{ICEBERG_ENDPOINT}/v1/config?warehouse={ICEBERG_WAREHOUSE}"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status == 200
        except urllib.error.HTTPError:
            return False
    except Exception:
        return False


iceberg_available = check_iceberg_available()


@pytest.mark.integration
@unittest.skipUnless(
    iceberg_available, f"Iceberg REST Catalog is not available at {ICEBERG_ENDPOINT}"
)
class TestIcebergNamespaceIntegration(unittest.TestCase):
    """Integration tests for IcebergNamespace against a running Iceberg REST Catalog."""

    def setUp(self):
        """Set up test fixtures."""
        unique_id = uuid.uuid4().hex[:8]
        self.test_warehouse = ICEBERG_WAREHOUSE
        self.test_namespace = f"test_ns_{unique_id}"

        properties = {
            "iceberg.endpoint": ICEBERG_ENDPOINT,
            "iceberg.root": "s3://warehouse",
        }

        self.namespace = IcebergNamespace(**properties)

    def tearDown(self):
        """Clean up test resources."""
        try:
            drop_request = DropNamespaceRequest()
            drop_request.id = [self.test_warehouse, self.test_namespace]
            self.namespace.drop_namespace(drop_request)
        except Exception:
            pass

        if self.namespace:
            self.namespace.close()

    def test_namespace_operations(self):
        """Test namespace CRUD operations."""
        # Create namespace
        create_request = CreateNamespaceRequest()
        create_request.id = [self.test_warehouse, self.test_namespace]
        create_request.properties = {"description": "Test namespace"}

        create_response = self.namespace.create_namespace(create_request)
        self.assertIsNotNone(create_response)

        # Describe namespace
        describe_request = DescribeNamespaceRequest()
        describe_request.id = [self.test_warehouse, self.test_namespace]

        describe_response = self.namespace.describe_namespace(describe_request)
        self.assertIsNotNone(describe_response)

        # Check namespace exists
        exists_request = NamespaceExistsRequest()
        exists_request.id = [self.test_warehouse, self.test_namespace]
        self.namespace.namespace_exists(exists_request)

        # List namespaces
        list_request = ListNamespacesRequest()
        list_request.id = [self.test_warehouse]
        list_response = self.namespace.list_namespaces(list_request)
        full_ns_name = f"{self.test_warehouse}.{self.test_namespace}"
        self.assertIn(full_ns_name, list_response.namespaces)

        # Drop namespace
        drop_request = DropNamespaceRequest()
        drop_request.id = [self.test_warehouse, self.test_namespace]
        self.namespace.drop_namespace(drop_request)

        # Verify namespace doesn't exist
        with self.assertRaises(NamespaceNotFoundException):
            self.namespace.namespace_exists(exists_request)

    def test_table_operations(self):
        """Test table CRUD operations."""
        # Create namespace first
        ns_request = CreateNamespaceRequest()
        ns_request.id = [self.test_warehouse, self.test_namespace]
        self.namespace.create_namespace(ns_request)

        table_name = f"test_table_{uuid.uuid4().hex[:8]}"

        # Create empty table
        create_request = CreateEmptyTableRequest()
        create_request.id = [self.test_warehouse, self.test_namespace, table_name]
        create_request.location = f"s3://warehouse/{self.test_namespace}/{table_name}"

        create_response = self.namespace.create_empty_table(create_request)
        self.assertIsNotNone(create_response.location)

        # Describe table
        describe_request = DescribeTableRequest()
        describe_request.id = [self.test_warehouse, self.test_namespace, table_name]

        describe_response = self.namespace.describe_table(describe_request)
        self.assertIsNotNone(describe_response.location)

        # Check table exists
        exists_request = TableExistsRequest()
        exists_request.id = [self.test_warehouse, self.test_namespace, table_name]
        self.namespace.table_exists(exists_request)

        # List tables
        list_request = ListTablesRequest()
        list_request.id = [self.test_warehouse, self.test_namespace]

        list_response = self.namespace.list_tables(list_request)
        self.assertIn(table_name, list_response.tables)

        # Deregister table
        deregister_request = DeregisterTableRequest()
        deregister_request.id = [self.test_warehouse, self.test_namespace, table_name]
        self.namespace.deregister_table(deregister_request)

        # Verify table doesn't exist
        with self.assertRaises(TableNotFoundException):
            self.namespace.table_exists(exists_request)

    def test_create_empty_table_with_location(self):
        """Test creating an empty table with a specific location."""
        # Create namespace first
        ns_request = CreateNamespaceRequest()
        ns_request.id = [self.test_warehouse, self.test_namespace]
        self.namespace.create_namespace(ns_request)

        table_name = "lance_table"
        create_request = CreateEmptyTableRequest()
        create_request.id = [self.test_warehouse, self.test_namespace, table_name]
        create_request.location = f"s3://warehouse/{self.test_namespace}/{table_name}"

        response = self.namespace.create_empty_table(create_request)
        self.assertIsNotNone(response.location)

        # Clean up table
        deregister_request = DeregisterTableRequest()
        deregister_request.id = [self.test_warehouse, self.test_namespace, table_name]
        self.namespace.deregister_table(deregister_request)

    def test_nested_namespace(self):
        """Test nested namespace operations."""
        nested_ns = f"nested_{uuid.uuid4().hex[:8]}"

        # Create parent namespace
        parent_request = CreateNamespaceRequest()
        parent_request.id = [self.test_warehouse, self.test_namespace]
        self.namespace.create_namespace(parent_request)

        # Create nested namespace
        nested_request = CreateNamespaceRequest()
        nested_request.id = [self.test_warehouse, self.test_namespace, nested_ns]
        nested_request.properties = {"description": "Nested namespace"}
        self.namespace.create_namespace(nested_request)

        # List nested namespaces
        list_request = ListNamespacesRequest()
        list_request.id = [self.test_warehouse, self.test_namespace]
        list_response = self.namespace.list_namespaces(list_request)
        expected_ns = f"{self.test_warehouse}.{self.test_namespace}.{nested_ns}"
        self.assertIn(expected_ns, list_response.namespaces)

        # Drop nested namespace first
        drop_nested = DropNamespaceRequest()
        drop_nested.id = [self.test_warehouse, self.test_namespace, nested_ns]
        self.namespace.drop_namespace(drop_nested)


if __name__ == "__main__":
    unittest.main()
