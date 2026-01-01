"""
Integration tests for Polaris Catalog namespace implementation.

To run these tests, start Polaris with:
  cd docker/polaris && docker-compose up -d

Tests are automatically skipped if Polaris is not available.
"""

import os
import uuid
import urllib.request
import urllib.error
import unittest

import pytest

from lance_namespace_impls.polaris import PolarisNamespace
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


POLARIS_ENDPOINT = os.environ.get("POLARIS_ENDPOINT", "http://localhost:8181")
CLIENT_ID = os.environ.get("POLARIS_CLIENT_ID", "root")
CLIENT_SECRET = os.environ.get("POLARIS_CLIENT_SECRET", "s3cr3t")


def check_polaris_available():
    """Check if Polaris is available."""
    try:
        url = f"{POLARIS_ENDPOINT}/api/catalog/v1/test_catalog/namespaces"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=2) as response:
                return response.status != 404
        except urllib.error.HTTPError as e:
            # 401/403 means server is up but needs auth, 404 means not found
            return e.code != 404 and e.code > 0
    except Exception:
        return False


def get_oauth_token():
    """Get OAuth token from Polaris."""
    try:
        url = f"{POLARIS_ENDPOINT}/api/catalog/v1/oauth/tokens"
        data = f"grant_type=client_credentials&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL"
        req = urllib.request.Request(
            url,
            data=data.encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            import json

            body = response.read().decode("utf-8")
            token_data = json.loads(body)
            return token_data.get("access_token")
    except Exception as e:
        print(f"Failed to get OAuth token: {e}")
        return None


polaris_available = check_polaris_available()


@pytest.mark.integration
@unittest.skipUnless(
    polaris_available, f"Polaris is not available at {POLARIS_ENDPOINT}"
)
class TestPolarisNamespaceIntegration(unittest.TestCase):
    """Integration tests for PolarisNamespace against a running Polaris instance."""

    @classmethod
    def setUpClass(cls):
        """Set up class-level resources."""
        cls.token = get_oauth_token()
        if not cls.token:
            raise unittest.SkipTest("Failed to get OAuth token from Polaris")

    def setUp(self):
        """Set up test fixtures."""
        unique_id = uuid.uuid4().hex[:8]
        self.test_catalog = "test_catalog"
        self.test_namespace = f"test_ns_{unique_id}"

        properties = {
            "polaris.endpoint": POLARIS_ENDPOINT,
            "polaris.auth_token": self.token,
            "polaris.root": "/data/warehouse",
        }

        self.namespace = PolarisNamespace(**properties)

    def tearDown(self):
        """Clean up test resources."""
        try:
            # Drop test namespace if it exists
            drop_request = DropNamespaceRequest()
            drop_request.id = [self.test_catalog, self.test_namespace]
            self.namespace.drop_namespace(drop_request)
        except Exception:
            pass

        if self.namespace:
            self.namespace.close()

    def test_namespace_operations(self):
        """Test namespace CRUD operations."""
        # Create namespace
        create_request = CreateNamespaceRequest()
        create_request.id = [self.test_catalog, self.test_namespace]
        create_request.properties = {"description": "Test namespace"}

        create_response = self.namespace.create_namespace(create_request)
        self.assertIsNotNone(create_response)

        # Describe namespace
        describe_request = DescribeNamespaceRequest()
        describe_request.id = [self.test_catalog, self.test_namespace]

        describe_response = self.namespace.describe_namespace(describe_request)
        self.assertIsNotNone(describe_response)

        # List namespaces
        list_request = ListNamespacesRequest()
        list_request.id = [self.test_catalog]
        list_response = self.namespace.list_namespaces(list_request)
        full_ns_name = f"{self.test_catalog}.{self.test_namespace}"
        self.assertIn(full_ns_name, list_response.namespaces)

        # Drop namespace
        drop_request = DropNamespaceRequest()
        drop_request.id = [self.test_catalog, self.test_namespace]
        self.namespace.drop_namespace(drop_request)

    def test_table_operations(self):
        """Test table CRUD operations."""
        # Create namespace first
        ns_request = CreateNamespaceRequest()
        ns_request.id = [self.test_catalog, self.test_namespace]
        self.namespace.create_namespace(ns_request)

        table_name = f"test_table_{uuid.uuid4().hex[:8]}"

        # Declare table
        create_request = DeclareTableRequest()
        create_request.id = [self.test_catalog, self.test_namespace, table_name]
        create_request.location = f"/data/warehouse/{self.test_namespace}/{table_name}"

        create_response = self.namespace.declare_table(create_request)
        self.assertIsNotNone(create_response.location)

        # Describe table
        describe_request = DescribeTableRequest()
        describe_request.id = [self.test_catalog, self.test_namespace, table_name]

        describe_response = self.namespace.describe_table(describe_request)
        self.assertIsNotNone(describe_response.location)

        # List tables
        list_request = ListTablesRequest()
        list_request.id = [self.test_catalog, self.test_namespace]

        list_response = self.namespace.list_tables(list_request)
        self.assertIn(table_name, list_response.tables)

        # Deregister table
        deregister_request = DeregisterTableRequest()
        deregister_request.id = [self.test_catalog, self.test_namespace, table_name]
        self.namespace.deregister_table(deregister_request)

    def test_declare_table_with_location(self):
        """Test declaring a table with a specific location."""
        # Create namespace first
        ns_request = CreateNamespaceRequest()
        ns_request.id = [self.test_catalog, self.test_namespace]
        self.namespace.create_namespace(ns_request)

        table_name = "lance_table"
        create_request = DeclareTableRequest()
        create_request.id = [self.test_catalog, self.test_namespace, table_name]
        create_request.location = f"/data/warehouse/{self.test_namespace}/{table_name}"

        response = self.namespace.declare_table(create_request)
        self.assertIsNotNone(response.location)

        # Clean up table
        deregister_request = DeregisterTableRequest()
        deregister_request.id = [self.test_catalog, self.test_namespace, table_name]
        self.namespace.deregister_table(deregister_request)


if __name__ == "__main__":
    unittest.main()
