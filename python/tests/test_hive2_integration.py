"""
Integration tests for Hive2 Namespace implementation.

To run these tests, start Hive2 Metastore with:
  cd docker/hive2 && docker-compose up -d

Tests are automatically skipped if Hive2 Metastore is not available.
"""

import os
import socket
import uuid
import unittest

import pytest

from lance_namespace_impls.hive2 import Hive2Namespace
from lance_namespace_urllib3_client.models import (
    CreateEmptyTableRequest,
    CreateNamespaceRequest,
    DeregisterTableRequest,
    DescribeNamespaceRequest,
    DescribeTableRequest,
    DropNamespaceRequest,
    ListNamespacesRequest,
    ListTablesRequest,
)


HIVE_HOST = os.environ.get("HIVE_HOST", "localhost")
HIVE_PORT = int(os.environ.get("HIVE_PORT", "9083"))
HIVE_URI = f"thrift://{HIVE_HOST}:{HIVE_PORT}"


def check_hive_available():
    """Check if Hive Metastore is available."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((HIVE_HOST, HIVE_PORT))
        sock.close()
        return result == 0
    except Exception:
        return False


hive_available = check_hive_available()


@pytest.mark.integration
@unittest.skipUnless(hive_available, f"Hive2 Metastore is not available at {HIVE_URI}")
class TestHive2NamespaceIntegration(unittest.TestCase):
    """Integration tests for Hive2Namespace against a running Hive2 Metastore."""

    def setUp(self):
        """Set up test fixtures."""
        unique_id = uuid.uuid4().hex[:8]
        self.test_database = f"test_db_{unique_id}"

        properties = {
            "uri": HIVE_URI,
            "root": "/tmp/lance",
        }

        self.namespace = Hive2Namespace(**properties)

    def tearDown(self):
        """Clean up test resources."""
        try:
            drop_request = DropNamespaceRequest()
            drop_request.id = [self.test_database]
            self.namespace.drop_namespace(drop_request)
        except Exception:
            pass

        if self.namespace:
            self.namespace.close()

    def test_list_databases(self):
        """Test listing databases at root level."""
        list_request = ListNamespacesRequest()
        list_request.id = []

        response = self.namespace.list_namespaces(list_request)

        # Should return a list of databases (may be empty initially)
        self.assertIsNotNone(response.namespaces)
        self.assertIsInstance(response.namespaces, list)

    def test_namespace_operations(self):
        """Test namespace CRUD operations."""
        # Create namespace (database)
        create_request = CreateNamespaceRequest()
        create_request.id = [self.test_database]
        create_request.properties = {"comment": "Test database for integration tests"}

        create_response = self.namespace.create_namespace(create_request)
        self.assertIsNotNone(create_response)

        # Describe namespace
        describe_request = DescribeNamespaceRequest()
        describe_request.id = [self.test_database]

        describe_response = self.namespace.describe_namespace(describe_request)
        self.assertIsNotNone(describe_response)
        self.assertEqual(
            describe_response.properties.get("comment"),
            "Test database for integration tests",
        )

        # List namespaces (databases)
        list_request = ListNamespacesRequest()
        list_request.id = []
        list_response = self.namespace.list_namespaces(list_request)
        self.assertIn(self.test_database, list_response.namespaces)

        # Drop namespace
        drop_request = DropNamespaceRequest()
        drop_request.id = [self.test_database]
        self.namespace.drop_namespace(drop_request)

    def test_table_operations(self):
        """Test table CRUD operations."""
        # Create namespace first
        ns_request = CreateNamespaceRequest()
        ns_request.id = [self.test_database]
        self.namespace.create_namespace(ns_request)

        table_name = f"test_table_{uuid.uuid4().hex[:8]}"

        # Create empty table (DeclareTable)
        create_request = CreateEmptyTableRequest()
        create_request.id = [self.test_database, table_name]
        create_request.location = f"/tmp/lance/{self.test_database}/{table_name}"

        create_response = self.namespace.create_empty_table(create_request)
        self.assertIsNotNone(create_response.location)

        # Describe table
        describe_request = DescribeTableRequest()
        describe_request.id = [self.test_database, table_name]

        describe_response = self.namespace.describe_table(describe_request)
        self.assertIsNotNone(describe_response.location)

        # List tables
        list_request = ListTablesRequest()
        list_request.id = [self.test_database]

        list_response = self.namespace.list_tables(list_request)
        self.assertIn(table_name, list_response.tables)

        # Deregister table
        deregister_request = DeregisterTableRequest()
        deregister_request.id = [self.test_database, table_name]
        self.namespace.deregister_table(deregister_request)

    def test_create_empty_table_with_location(self):
        """Test creating an empty table with a specific location."""
        # Create namespace first
        ns_request = CreateNamespaceRequest()
        ns_request.id = [self.test_database]
        self.namespace.create_namespace(ns_request)

        table_name = "lance_table"
        create_request = CreateEmptyTableRequest()
        create_request.id = [self.test_database, table_name]
        create_request.location = f"/tmp/lance/{self.test_database}/{table_name}"

        response = self.namespace.create_empty_table(create_request)
        self.assertIsNotNone(response.location)

        # Clean up table
        deregister_request = DeregisterTableRequest()
        deregister_request.id = [self.test_database, table_name]
        self.namespace.deregister_table(deregister_request)


if __name__ == "__main__":
    unittest.main()
