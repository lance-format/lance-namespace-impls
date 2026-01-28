"""
Integration tests for GooseFS Namespace implementation.

To run these tests, start GooseFS Table Master with:
  cd docker && docker-compose up -d goosefs

Tests are automatically skipped if GooseFS is not available.
"""

import os
import socket
import uuid
import unittest

import pytest

from lance_namespace_impls.goosefs import GooseFSNamespace, GOOSEFS_CLIENT_AVAILABLE
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


GOOSEFS_HOST = os.environ.get("GOOSEFS_HOST", "localhost")
GOOSEFS_PORT = int(os.environ.get("GOOSEFS_PORT", "9220"))
GOOSEFS_URI = f"goosefs://{GOOSEFS_HOST}:{GOOSEFS_PORT}"


def check_goosefs_available():
    """Check if GooseFS Table Master is available."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((GOOSEFS_HOST, GOOSEFS_PORT))
        sock.close()
        return result == 0
    except Exception:
        return False


goosefs_available = check_goosefs_available()


@pytest.mark.integration
@unittest.skipUnless(
    GOOSEFS_CLIENT_AVAILABLE and goosefs_available,
    f"GooseFS dependencies not installed or Table Master not available at {GOOSEFS_URI}",
)
class TestGooseFSNamespaceIntegration(unittest.TestCase):
    """Integration tests for GooseFSNamespace against a running GooseFS Table Master."""

    def setUp(self):
        """Set up test fixtures."""
        unique_id = uuid.uuid4().hex[:8]
        self.test_database = f"test_db_{unique_id}"

        properties = {
            "uri": GOOSEFS_URI,
            "root": "/tmp/lance",
        }

        self.namespace = GooseFSNamespace(**properties)

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

        self.assertIsNotNone(response.namespaces)
        self.assertIsInstance(response.namespaces, list)

    def test_describe_root_namespace(self):
        """Test describing root namespace."""
        describe_request = DescribeNamespaceRequest()
        describe_request.id = []

        response = self.namespace.describe_namespace(describe_request)

        self.assertIsNotNone(response.properties)
        self.assertEqual(response.properties["location"], "/tmp/lance")
        self.assertEqual(response.properties["host"], GOOSEFS_HOST)
        self.assertEqual(response.properties["port"], str(GOOSEFS_PORT))

    def test_namespace_operations(self):
        """Test namespace CRUD operations."""
        create_request = CreateNamespaceRequest()
        create_request.id = [self.test_database]
        create_request.properties = {
            "udb_type": "hive",
            "udb_db_name": "default",
            "ignore_sync_errors": "true",
        }

        create_response = self.namespace.create_namespace(create_request)
        self.assertIsNotNone(create_response)

        describe_request = DescribeNamespaceRequest()
        describe_request.id = [self.test_database]

        describe_response = self.namespace.describe_namespace(describe_request)
        self.assertIsNotNone(describe_response)
        self.assertEqual(describe_response.properties.get("db_name"), self.test_database)

        list_request = ListNamespacesRequest()
        list_request.id = []
        list_response = self.namespace.list_namespaces(list_request)
        self.assertIn(self.test_database, list_response.namespaces)

        drop_request = DropNamespaceRequest()
        drop_request.id = [self.test_database]
        self.namespace.drop_namespace(drop_request)

    def test_table_operations(self):
        """Test table CRUD operations."""
        ns_request = CreateNamespaceRequest()
        ns_request.id = [self.test_database]
        ns_request.properties = {
            "udb_type": "hive",
            "udb_db_name": "default",
            "ignore_sync_errors": "true",
        }
        self.namespace.create_namespace(ns_request)

        table_name = f"test_table_{uuid.uuid4().hex[:8]}"

        create_request = DeclareTableRequest()
        create_request.id = [self.test_database, table_name]
        create_request.location = f"/tmp/lance/{self.test_database}/{table_name}"

        create_response = self.namespace.declare_table(create_request)
        self.assertIsNotNone(create_response.location)

        describe_request = DescribeTableRequest()
        describe_request.id = [self.test_database, table_name]

        try:
            describe_response = self.namespace.describe_table(describe_request)
            self.assertIsNotNone(describe_response.location)
        except Exception:
            pass

        list_request = ListTablesRequest()
        list_request.id = [self.test_database]

        list_response = self.namespace.list_tables(list_request)
        self.assertIsInstance(list_response.tables, list)

        deregister_request = DeregisterTableRequest()
        deregister_request.id = [self.test_database, table_name]

        try:
            self.namespace.deregister_table(deregister_request)
        except Exception:
            pass

    def test_declare_table_with_location(self):
        """Test declaring a table with a specific location."""
        ns_request = CreateNamespaceRequest()
        ns_request.id = [self.test_database]
        ns_request.properties = {
            "udb_type": "hive",
            "udb_db_name": "default",
            "ignore_sync_errors": "true",
        }
        self.namespace.create_namespace(ns_request)

        table_name = "lance_table"
        create_request = DeclareTableRequest()
        create_request.id = [self.test_database, table_name]
        create_request.location = f"/tmp/lance/{self.test_database}/{table_name}"

        response = self.namespace.declare_table(create_request)
        self.assertIsNotNone(response.location)

        deregister_request = DeregisterTableRequest()
        deregister_request.id = [self.test_database, table_name]

        try:
            self.namespace.deregister_table(deregister_request)
        except Exception:
            pass

    def test_sync_database(self):
        """Test syncing a database."""
        ns_request = CreateNamespaceRequest()
        ns_request.id = [self.test_database]
        ns_request.properties = {
            "udb_type": "hive",
            "udb_db_name": "default",
            "ignore_sync_errors": "true",
        }
        self.namespace.create_namespace(ns_request)

        result = self.namespace.sync_database(self.test_database)
        self.assertIsNotNone(result)
        self.assertIn("tables_updated", result)
        self.assertIn("tables_removed", result)


if __name__ == "__main__":
    unittest.main()
