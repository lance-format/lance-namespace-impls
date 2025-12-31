"""
Integration tests for Unity Catalog namespace implementation.

To run these tests, start Unity Catalog with:
  cd docker/unity && docker-compose up -d

Tests are automatically skipped if Unity Catalog is not available.
"""

import os
import uuid
import urllib.request
import urllib.error
import unittest

import pytest

from lance_namespace_impls.unity import UnityNamespace
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


UNITY_ENDPOINT = os.environ.get("UNITY_ENDPOINT", "http://localhost:8080")
UNITY_CATALOG = os.environ.get("UNITY_CATALOG", "lance_test")


def check_unity_available():
    """Check if Unity Catalog is available."""
    try:
        url = f"{UNITY_ENDPOINT}/api/2.1/unity-catalog/catalogs"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=2) as response:
                return response.status == 200
        except urllib.error.HTTPError as e:
            return e.code != 404 and e.code > 0
    except Exception:
        return False


def check_catalog_exists():
    """Check if the test catalog exists."""
    try:
        url = f"{UNITY_ENDPOINT}/api/2.1/unity-catalog/catalogs/{UNITY_CATALOG}"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=2) as response:
            return response.status == 200
    except Exception:
        return False


unity_available = check_unity_available()


@pytest.mark.integration
@unittest.skipUnless(
    unity_available, f"Unity Catalog is not available at {UNITY_ENDPOINT}"
)
class TestUnityNamespaceIntegration(unittest.TestCase):
    """Integration tests for UnityNamespace against a running Unity Catalog instance."""

    @classmethod
    def setUpClass(cls):
        """Set up class-level resources."""
        if not check_catalog_exists():
            raise unittest.SkipTest(
                f"Test catalog '{UNITY_CATALOG}' does not exist in Unity Catalog"
            )

    def setUp(self):
        """Set up test fixtures."""
        unique_id = uuid.uuid4().hex[:8]
        self.test_schema = f"test_schema_{unique_id}"

        properties = {
            "unity.endpoint": UNITY_ENDPOINT,
            "unity.root": "/tmp/lance",
        }

        self.namespace = UnityNamespace(**properties)

    def tearDown(self):
        """Clean up test resources."""
        try:
            # Drop test schema if it exists
            drop_request = DropNamespaceRequest()
            drop_request.id = [UNITY_CATALOG, self.test_schema]
            self.namespace.drop_namespace(drop_request)
        except Exception:
            pass

        if self.namespace:
            self.namespace.close()

    def test_list_catalogs(self):
        """Test listing catalogs at root level."""
        list_request = ListNamespacesRequest()
        list_request.id = []

        response = self.namespace.list_namespaces(list_request)

        # Should list all catalogs including our test catalog
        self.assertIn(UNITY_CATALOG, response.namespaces)

    def test_namespace_operations(self):
        """Test namespace CRUD operations."""
        # Create namespace (schema)
        create_request = CreateNamespaceRequest()
        create_request.id = [UNITY_CATALOG, self.test_schema]
        create_request.properties = {}

        create_response = self.namespace.create_namespace(create_request)
        self.assertIsNotNone(create_response)

        # Describe namespace
        describe_request = DescribeNamespaceRequest()
        describe_request.id = [UNITY_CATALOG, self.test_schema]

        describe_response = self.namespace.describe_namespace(describe_request)
        self.assertIsNotNone(describe_response)

        # List namespaces (schemas)
        list_request = ListNamespacesRequest()
        list_request.id = [UNITY_CATALOG]
        list_response = self.namespace.list_namespaces(list_request)
        self.assertIn(self.test_schema, list_response.namespaces)

        # Drop namespace
        drop_request = DropNamespaceRequest()
        drop_request.id = [UNITY_CATALOG, self.test_schema]
        self.namespace.drop_namespace(drop_request)

    def test_table_operations(self):
        """Test table CRUD operations."""
        # Create namespace first
        ns_request = CreateNamespaceRequest()
        ns_request.id = [UNITY_CATALOG, self.test_schema]
        self.namespace.create_namespace(ns_request)

        table_name = f"test_table_{uuid.uuid4().hex[:8]}"

        # Declare table
        create_request = DeclareTableRequest()
        create_request.id = [UNITY_CATALOG, self.test_schema, table_name]
        create_request.location = (
            f"/tmp/lance/{UNITY_CATALOG}/{self.test_schema}/{table_name}"
        )

        create_response = self.namespace.declare_table(create_request)
        self.assertIsNotNone(create_response.location)

        # Describe table
        describe_request = DescribeTableRequest()
        describe_request.id = [UNITY_CATALOG, self.test_schema, table_name]

        describe_response = self.namespace.describe_table(describe_request)
        self.assertIsNotNone(describe_response.location)

        # List tables
        list_request = ListTablesRequest()
        list_request.id = [UNITY_CATALOG, self.test_schema]

        list_response = self.namespace.list_tables(list_request)
        self.assertIn(table_name, list_response.tables)

        # Deregister table
        deregister_request = DeregisterTableRequest()
        deregister_request.id = [UNITY_CATALOG, self.test_schema, table_name]
        self.namespace.deregister_table(deregister_request)

    def test_declare_table_with_location(self):
        """Test declaring a table with a specific location."""
        # Create namespace first
        ns_request = CreateNamespaceRequest()
        ns_request.id = [UNITY_CATALOG, self.test_schema]
        self.namespace.create_namespace(ns_request)

        table_name = "lance_table"
        create_request = DeclareTableRequest()
        create_request.id = [UNITY_CATALOG, self.test_schema, table_name]
        create_request.location = (
            f"/tmp/lance/{UNITY_CATALOG}/{self.test_schema}/{table_name}"
        )

        response = self.namespace.declare_table(create_request)
        self.assertIsNotNone(response.location)

        # Clean up table
        deregister_request = DeregisterTableRequest()
        deregister_request.id = [UNITY_CATALOG, self.test_schema, table_name]
        self.namespace.deregister_table(deregister_request)


if __name__ == "__main__":
    unittest.main()
