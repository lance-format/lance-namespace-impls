"""
Tests for Iceberg REST Catalog namespace implementation.
"""

import unittest
from unittest.mock import MagicMock, patch

from lance_namespace_impls.iceberg import (
    IcebergNamespace,
    IcebergNamespaceConfig,
    create_dummy_schema,
)
from lance_namespace_impls.rest_client import (
    RestClientException,
    NamespaceNotFoundException,
    NamespaceAlreadyExistsException,
    TableNotFoundException,
    TableAlreadyExistsException,
    InvalidInputException,
)
from lance_namespace_urllib3_client.models import (
    ListNamespacesRequest,
    CreateNamespaceRequest,
    DescribeNamespaceRequest,
    DropNamespaceRequest,
    ListTablesRequest,
    CreateEmptyTableRequest,
    DescribeTableRequest,
    DeregisterTableRequest,
)


class TestIcebergNamespaceConfig(unittest.TestCase):
    """Test Iceberg namespace configuration."""

    def test_config_initialization(self):
        """Test configuration initialization with required properties."""
        properties = {
            "iceberg.endpoint": "https://iceberg.example.com",
            "iceberg.root": "/data/lance",
            "iceberg.auth_token": "test_token",
            "iceberg.warehouse": "test_warehouse",
        }

        config = IcebergNamespaceConfig(properties)

        self.assertEqual(config.endpoint, "https://iceberg.example.com")
        self.assertEqual(config.root, "/data/lance")
        self.assertEqual(config.auth_token, "test_token")
        self.assertEqual(config.warehouse, "test_warehouse")

    def test_config_defaults(self):
        """Test configuration with default values."""
        properties = {"iceberg.endpoint": "https://iceberg.example.com"}

        config = IcebergNamespaceConfig(properties)

        self.assertEqual(config.root, "/tmp/lance")
        self.assertIsNone(config.auth_token)
        self.assertIsNone(config.warehouse)
        self.assertEqual(config.connect_timeout, 10000)
        self.assertEqual(config.read_timeout, 30000)
        self.assertEqual(config.max_retries, 3)

    def test_config_missing_endpoint(self):
        """Test configuration fails without endpoint."""
        properties = {}

        with self.assertRaises(ValueError) as context:
            IcebergNamespaceConfig(properties)

        self.assertIn("iceberg.endpoint", str(context.exception))

    def test_get_base_api_url(self):
        """Test API URL generation."""
        properties = {"iceberg.endpoint": "https://iceberg.example.com/"}
        config = IcebergNamespaceConfig(properties)

        self.assertEqual(config.get_base_api_url(), "https://iceberg.example.com")


class TestIcebergNamespace(unittest.TestCase):
    """Test Iceberg namespace implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.properties = {
            "iceberg.endpoint": "https://iceberg.example.com",
            "iceberg.root": "/data/lance",
        }

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_namespace_id(self, mock_rest_client_class):
        """Test namespace ID generation."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        namespace = IcebergNamespace(**self.properties)
        ns_id = namespace.namespace_id()

        self.assertIn("IcebergNamespace", ns_id)
        self.assertIn("iceberg.example.com", ns_id)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_list_namespaces_prefix_level(self, mock_rest_client_class):
        """Test listing namespaces at prefix level."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = [
            {"defaults": {"prefix": "warehouse1"}},
            {"namespaces": [["ns1"], ["ns2"], ["ns3"]]},
        ]

        namespace = IcebergNamespace(**self.properties)

        request = ListNamespacesRequest()
        request.id = ["warehouse1"]

        response = namespace.list_namespaces(request)

        self.assertEqual(
            sorted(response.namespaces),
            ["warehouse1.ns1", "warehouse1.ns2", "warehouse1.ns3"],
        )

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_list_namespaces_nested(self, mock_rest_client_class):
        """Test listing nested namespaces."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = [
            {"defaults": {"prefix": "warehouse1"}},
            {"namespaces": [["parent", "child1"], ["parent", "child2"]]},
        ]

        namespace = IcebergNamespace(**self.properties)

        request = ListNamespacesRequest()
        request.id = ["warehouse1", "parent"]

        response = namespace.list_namespaces(request)

        self.assertEqual(
            sorted(response.namespaces),
            ["warehouse1.parent.child1", "warehouse1.parent.child2"],
        )

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_list_namespaces_empty_id(self, mock_rest_client_class):
        """Test listing namespaces without prefix fails."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        namespace = IcebergNamespace(**self.properties)

        request = ListNamespacesRequest()
        request.id = []

        with self.assertRaises(InvalidInputException):
            namespace.list_namespaces(request)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_create_namespace(self, mock_rest_client_class):
        """Test creating a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {"defaults": {"prefix": "warehouse1"}}
        mock_client.post.return_value = {"properties": {"key": "value"}}

        namespace = IcebergNamespace(**self.properties)

        request = CreateNamespaceRequest()
        request.id = ["warehouse1", "test_namespace"]
        request.properties = {"key": "value"}

        response = namespace.create_namespace(request)

        self.assertEqual(response.properties, {"key": "value"})
        mock_client.post.assert_called_once_with(
            "/v1/warehouse1/namespaces",
            {"namespace": ["test_namespace"], "properties": {"key": "value"}},
        )

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_create_namespace_already_exists(self, mock_rest_client_class):
        """Test creating a namespace that already exists."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {"defaults": {"prefix": "warehouse1"}}
        mock_client.post.side_effect = RestClientException(
            status_code=409, response_body="Conflict"
        )

        namespace = IcebergNamespace(**self.properties)

        request = CreateNamespaceRequest()
        request.id = ["warehouse1", "existing_namespace"]

        with self.assertRaises(NamespaceAlreadyExistsException):
            namespace.create_namespace(request)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_create_namespace_invalid_id(self, mock_rest_client_class):
        """Test creating namespace with invalid ID fails."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        namespace = IcebergNamespace(**self.properties)

        request = CreateNamespaceRequest()
        request.id = ["only_prefix"]

        with self.assertRaises(InvalidInputException):
            namespace.create_namespace(request)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_describe_namespace(self, mock_rest_client_class):
        """Test describing a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = [
            {"defaults": {"prefix": "warehouse1"}},
            {"properties": {"key": "value"}},
        ]

        namespace = IcebergNamespace(**self.properties)

        request = DescribeNamespaceRequest()
        request.id = ["warehouse1", "test_namespace"]

        response = namespace.describe_namespace(request)

        self.assertEqual(response.properties, {"key": "value"})

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_describe_namespace_not_found(self, mock_rest_client_class):
        """Test describing a namespace that doesn't exist."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = [
            {"defaults": {"prefix": "warehouse1"}},
            RestClientException(status_code=404, response_body="Not found"),
        ]

        namespace = IcebergNamespace(**self.properties)

        request = DescribeNamespaceRequest()
        request.id = ["warehouse1", "nonexistent"]

        with self.assertRaises(NamespaceNotFoundException):
            namespace.describe_namespace(request)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_drop_namespace(self, mock_rest_client_class):
        """Test dropping a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {"defaults": {"prefix": "warehouse1"}}

        namespace = IcebergNamespace(**self.properties)

        request = DropNamespaceRequest()
        request.id = ["warehouse1", "test_namespace"]

        response = namespace.drop_namespace(request)

        self.assertIsNotNone(response)
        mock_client.delete.assert_called_once()

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_drop_namespace_not_found(self, mock_rest_client_class):
        """Test dropping a namespace that doesn't exist returns success."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {"defaults": {"prefix": "warehouse1"}}
        mock_client.delete.side_effect = RestClientException(
            status_code=404, response_body="Not found"
        )

        namespace = IcebergNamespace(**self.properties)

        request = DropNamespaceRequest()
        request.id = ["warehouse1", "nonexistent"]

        response = namespace.drop_namespace(request)

        self.assertIsNotNone(response)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_list_tables(self, mock_rest_client_class):
        """Test listing tables in a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = [
            {"defaults": {"prefix": "warehouse1"}},
            {"identifiers": [{"name": "table1"}, {"name": "table2"}]},
            {"metadata": {"properties": {"table_type": "lance"}}},
            {"metadata": {"properties": {"table_type": "lance"}}},
        ]

        namespace = IcebergNamespace(**self.properties)

        request = ListTablesRequest()
        request.id = ["warehouse1", "test_namespace"]

        response = namespace.list_tables(request)

        self.assertEqual(sorted(response.tables), ["table1", "table2"])

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_list_tables_invalid_id(self, mock_rest_client_class):
        """Test listing tables with invalid ID fails."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        namespace = IcebergNamespace(**self.properties)

        request = ListTablesRequest()
        request.id = ["only_prefix"]

        with self.assertRaises(InvalidInputException):
            namespace.list_tables(request)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_create_empty_table(self, mock_rest_client_class):
        """Test creating an empty table."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {"defaults": {"prefix": "warehouse1"}}
        mock_client.post.return_value = {}

        namespace = IcebergNamespace(**self.properties)

        request = CreateEmptyTableRequest()
        request.id = ["warehouse1", "test_namespace", "test_table"]
        request.location = None

        response = namespace.create_empty_table(request)

        self.assertEqual(
            response.location, "/data/lance/warehouse1/test_namespace/test_table"
        )
        mock_client.post.assert_called_once()

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_create_empty_table_with_location(self, mock_rest_client_class):
        """Test creating an empty table with custom location."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {"defaults": {"prefix": "warehouse1"}}
        mock_client.post.return_value = {}

        namespace = IcebergNamespace(**self.properties)

        request = CreateEmptyTableRequest()
        request.id = ["warehouse1", "test_namespace", "test_table"]
        request.location = "/custom/path/test_table"

        response = namespace.create_empty_table(request)

        self.assertEqual(response.location, "/custom/path/test_table")

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_create_empty_table_already_exists(self, mock_rest_client_class):
        """Test creating a table that already exists."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {"defaults": {"prefix": "warehouse1"}}
        mock_client.post.side_effect = RestClientException(
            status_code=409, response_body="Conflict"
        )

        namespace = IcebergNamespace(**self.properties)

        request = CreateEmptyTableRequest()
        request.id = ["warehouse1", "test_namespace", "existing_table"]

        with self.assertRaises(TableAlreadyExistsException):
            namespace.create_empty_table(request)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_create_empty_table_invalid_id(self, mock_rest_client_class):
        """Test creating table with invalid ID fails."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        namespace = IcebergNamespace(**self.properties)

        request = CreateEmptyTableRequest()
        request.id = ["warehouse1", "only_namespace"]

        with self.assertRaises(InvalidInputException):
            namespace.create_empty_table(request)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_describe_table(self, mock_rest_client_class):
        """Test describing a table."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = [
            {"defaults": {"prefix": "warehouse1"}},
            {
                "metadata": {
                    "location": "/data/lance/ns/table",
                    "properties": {"table_type": "lance", "key": "value"},
                }
            },
        ]

        namespace = IcebergNamespace(**self.properties)

        request = DescribeTableRequest()
        request.id = ["warehouse1", "test_namespace", "test_table"]

        response = namespace.describe_table(request)

        self.assertEqual(response.location, "/data/lance/ns/table")
        self.assertEqual(
            response.storage_options, {"table_type": "lance", "key": "value"}
        )

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_describe_table_not_lance(self, mock_rest_client_class):
        """Test describing a table that is not a Lance table."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = [
            {"defaults": {"prefix": "warehouse1"}},
            {
                "metadata": {
                    "location": "/data/iceberg/ns/table",
                    "properties": {"table_type": "iceberg"},
                }
            },
        ]

        namespace = IcebergNamespace(**self.properties)

        request = DescribeTableRequest()
        request.id = ["warehouse1", "test_namespace", "test_table"]

        with self.assertRaises(InvalidInputException):
            namespace.describe_table(request)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_describe_table_not_found(self, mock_rest_client_class):
        """Test describing a table that doesn't exist."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = [
            {"defaults": {"prefix": "warehouse1"}},
            RestClientException(status_code=404, response_body="Not found"),
        ]

        namespace = IcebergNamespace(**self.properties)

        request = DescribeTableRequest()
        request.id = ["warehouse1", "test_namespace", "nonexistent"]

        with self.assertRaises(TableNotFoundException):
            namespace.describe_table(request)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_deregister_table(self, mock_rest_client_class):
        """Test deregistering a table."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = [
            {"defaults": {"prefix": "warehouse1"}},
            {"metadata": {"location": "/data/lance/ns/table"}},
        ]

        namespace = IcebergNamespace(**self.properties)

        request = DeregisterTableRequest()
        request.id = ["warehouse1", "test_namespace", "test_table"]

        response = namespace.deregister_table(request)

        self.assertEqual(response.location, "/data/lance/ns/table")
        mock_client.delete.assert_called_once()

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_deregister_table_not_found(self, mock_rest_client_class):
        """Test deregistering a table that doesn't exist."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = [
            {"defaults": {"prefix": "warehouse1"}},
            RestClientException(status_code=404, response_body="Not found"),
        ]

        namespace = IcebergNamespace(**self.properties)

        request = DeregisterTableRequest()
        request.id = ["warehouse1", "test_namespace", "nonexistent"]

        with self.assertRaises(TableNotFoundException):
            namespace.deregister_table(request)

    @patch("lance_namespace_impls.iceberg.RestClient")
    def test_close(self, mock_rest_client_class):
        """Test closing the namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        namespace = IcebergNamespace(**self.properties)
        namespace.close()

        mock_client.close.assert_called_once()

    def test_create_dummy_schema(self):
        """Test dummy schema creation."""
        schema = create_dummy_schema()

        self.assertEqual(schema["type"], "struct")
        self.assertEqual(schema["schema-id"], 0)
        self.assertEqual(len(schema["fields"]), 1)
        self.assertEqual(schema["fields"][0]["name"], "dummy")


if __name__ == "__main__":
    unittest.main()
