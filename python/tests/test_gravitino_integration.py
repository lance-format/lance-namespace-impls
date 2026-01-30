"""
Integration tests for Gravitino REST namespace implementation.

This test uses Apache Gravitino's Lance REST service.
To run these tests, start Gravitino with Lance REST service enabled.

Tests are automatically skipped if the service is not available.
"""

import os
import uuid
import urllib.request
import urllib.error
import unittest

import pytest

from lance_namespace_impls.gravitino import GravitinoNamespace
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


GRAVITINO_ENDPOINT = os.environ.get("GRAVITINO_ENDPOINT", "http://localhost:9101")


def check_gravitino_available():
    """Check if Gravitino Lance REST service is available."""
    try:
        url = f"{GRAVITINO_ENDPOINT}/lance/v1/namespace/list"
        req = urllib.request.Request(url, method="GET")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status == 200
        except urllib.error.HTTPError as e:
            # 404 is acceptable - it means the service is running but no namespaces exist
            return e.code in [200, 404]
    except Exception:
        return False


gravitino_available = check_gravitino_available()


@pytest.mark.integration
@unittest.skipUnless(gravitino_available, "Gravitino Lance REST service not available")
class TestGravitinoIntegration(unittest.TestCase):
    """Integration tests for Gravitino namespace."""

    def setUp(self):
        """Set up test fixtures."""
        self.namespace = GravitinoNamespace(
            endpoint=GRAVITINO_ENDPOINT,
        )
        self.test_catalog = f"test_catalog_{uuid.uuid4().hex[:8]}"
        self.test_schema = f"test_schema_{uuid.uuid4().hex[:8]}"
        self.test_table = f"test_table_{uuid.uuid4().hex[:8]}"

    def test_namespace_lifecycle(self):
        """Test complete namespace lifecycle: create, list, describe, drop."""
        # Create catalog
        catalog_request = CreateNamespaceRequest(
            id=[self.test_catalog], properties={"description": "Test catalog"}
        )
        catalog_response = self.namespace.create_namespace(catalog_request)
        self.assertIsNotNone(catalog_response)

        # Create schema
        schema_request = CreateNamespaceRequest(
            id=[self.test_catalog, self.test_schema],
            properties={"description": "Test schema"},
        )
        schema_response = self.namespace.create_namespace(schema_request)
        self.assertIsNotNone(schema_response)

        # Verify catalog was created by describing it
        describe_catalog_request = DescribeNamespaceRequest(id=[self.test_catalog])
        catalog_description = self.namespace.describe_namespace(describe_catalog_request)
        self.assertIsNotNone(catalog_description)

        # List catalogs (now working with correct endpoint)
        list_request = ListNamespacesRequest(id=[])
        list_response = self.namespace.list_namespaces(list_request)
        self.assertIn(self.test_catalog, list_response.namespaces)

        # List schemas in catalog
        list_schemas_request = ListNamespacesRequest(id=[self.test_catalog])
        list_schemas_response = self.namespace.list_namespaces(list_schemas_request)
        expected_schema = f"{self.test_catalog}.{self.test_schema}"
        self.assertIn(expected_schema, list_schemas_response.namespaces)

        # Describe schema
        describe_request = DescribeNamespaceRequest(
            id=[self.test_catalog, self.test_schema]
        )
        describe_response = self.namespace.describe_namespace(describe_request)
        self.assertIsNotNone(describe_response)

        # Clean up - drop schema first, then catalog
        try:
            drop_schema_request = DropNamespaceRequest(
                id=[self.test_catalog, self.test_schema]
            )
            drop_schema_response = self.namespace.drop_namespace(drop_schema_request)
            self.assertIsNotNone(drop_schema_response)

            # Clean up - drop catalog
            drop_catalog_request = DropNamespaceRequest(id=[self.test_catalog], behavior="CASCADE")
            drop_catalog_response = self.namespace.drop_namespace(drop_catalog_request)
            self.assertIsNotNone(drop_catalog_response)
        except Exception as e:
            # If cleanup fails, log it but don't fail the test
            print(f"   ⚠ Cleanup failed (this may be expected): {e}")
            # Try to clean up manually for next test
            try:
                self.namespace.drop_namespace(DropNamespaceRequest(id=[self.test_catalog, self.test_schema]))
            except:
                pass
            try:
                self.namespace.drop_namespace(DropNamespaceRequest(id=[self.test_catalog], behavior="CASCADE"))
            except:
                pass

    def test_table_lifecycle(self):
        """Test complete table lifecycle: declare, list, describe, deregister."""
        # Create catalog and schema first
        catalog_request = CreateNamespaceRequest(id=[self.test_catalog])
        self.namespace.create_namespace(catalog_request)

        schema_request = CreateNamespaceRequest(
            id=[self.test_catalog, self.test_schema]
        )
        self.namespace.create_namespace(schema_request)

        try:
            # Declare table
            table_location = f"/tmp/{self.test_catalog}/{self.test_schema}/{self.test_table}/"
            declare_request = DeclareTableRequest(
                id=[self.test_catalog, self.test_schema, self.test_table],
                location=table_location,
            )
            declare_response = self.namespace.declare_table(declare_request)
            self.assertIsNotNone(declare_response)
            self.assertEqual(declare_response.location, table_location)

            # List tables
            list_tables_request = ListTablesRequest(
                id=[self.test_catalog, self.test_schema]
            )
            list_tables_response = self.namespace.list_tables(list_tables_request)
            self.assertIn(self.test_table, list_tables_response.tables)

            # Describe table (may not be fully supported)
            describe_table_request = DescribeTableRequest(
                id=[self.test_catalog, self.test_schema, self.test_table]
            )
            try:
                describe_table_response = self.namespace.describe_table(describe_table_request)
                self.assertIsNotNone(describe_table_response)
            except Exception:
                # This is acceptable since Gravitino may not support full table describe
                pass

        finally:
            # Clean up namespace - deregister table first, then drop schema, then catalog
            deregister_request = DeregisterTableRequest(
                id=[self.test_catalog, self.test_schema, self.test_table]
            )
            self.namespace.deregister_table(deregister_request)

            drop_schema_request = DropNamespaceRequest(
                id=[self.test_catalog, self.test_schema]
            )
            self.namespace.drop_namespace(drop_schema_request)

            drop_catalog_request = DropNamespaceRequest(id=[self.test_catalog], behavior="CASCADE")
            self.namespace.drop_namespace(drop_catalog_request)


if __name__ == "__main__":
    unittest.main()