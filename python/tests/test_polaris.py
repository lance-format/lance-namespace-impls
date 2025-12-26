"""
Tests for Polaris Catalog namespace implementation.
"""

import unittest
from unittest.mock import MagicMock, patch

from lance_namespace_impls.polaris import (
    PolarisNamespace,
    PolarisNamespaceConfig,
)
from lance_namespace_impls.rest_client import (
    RestClientException,
    NamespaceNotFoundException,
    NamespaceAlreadyExistsException,
    TableNotFoundException,
    TableAlreadyExistsException,
    InvalidInputException,
    InternalException,
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


class TestPolarisNamespaceConfig(unittest.TestCase):
    """Test Polaris namespace configuration."""

    def test_config_initialization(self):
        """Test configuration initialization with required properties."""
        properties = {
            "polaris.endpoint": "https://polaris.example.com",
            "polaris.root": "/data/lance",
            "polaris.auth_token": "test_token",
        }

        config = PolarisNamespaceConfig(properties)

        self.assertEqual(config.endpoint, "https://polaris.example.com")
        self.assertEqual(config.root, "/data/lance")
        self.assertEqual(config.auth_token, "test_token")

    def test_config_defaults(self):
        """Test configuration with default values."""
        properties = {"polaris.endpoint": "https://polaris.example.com"}

        config = PolarisNamespaceConfig(properties)

        self.assertEqual(config.root, "/tmp/lance")
        self.assertIsNone(config.auth_token)
        self.assertEqual(config.connect_timeout, 10000)
        self.assertEqual(config.read_timeout, 30000)
        self.assertEqual(config.max_retries, 3)

    def test_config_missing_endpoint(self):
        """Test configuration fails without endpoint."""
        properties = {}

        with self.assertRaises(ValueError) as context:
            PolarisNamespaceConfig(properties)

        self.assertIn("polaris.endpoint", str(context.exception))

    def test_get_full_api_url(self):
        """Test API URL generation."""
        properties = {"polaris.endpoint": "https://polaris.example.com/"}
        config = PolarisNamespaceConfig(properties)

        self.assertEqual(config.get_full_api_url(), "https://polaris.example.com")


class TestPolarisNamespace(unittest.TestCase):
    """Test Polaris namespace implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.properties = {
            "polaris.endpoint": "https://polaris.example.com",
            "polaris.root": "/data/lance",
        }

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_namespace_id(self, mock_rest_client_class):
        """Test namespace ID generation."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        namespace = PolarisNamespace(**self.properties)
        ns_id = namespace.namespace_id()

        self.assertIn("PolarisNamespace", ns_id)
        self.assertIn("polaris.example.com", ns_id)

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_list_namespaces_top_level(self, mock_rest_client_class):
        """Test listing top-level namespaces."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {
            "namespaces": [["ns1"], ["ns2"], ["ns3"]]
        }

        namespace = PolarisNamespace(**self.properties)

        request = ListNamespacesRequest()
        request.id = []

        response = namespace.list_namespaces(request)

        self.assertEqual(sorted(response.namespaces), ["ns1", "ns2", "ns3"])
        mock_client.get.assert_called_once_with("/namespaces")

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_list_namespaces_nested(self, mock_rest_client_class):
        """Test listing nested namespaces."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {
            "namespaces": [["parent", "child1"], ["parent", "child2"]]
        }

        namespace = PolarisNamespace(**self.properties)

        request = ListNamespacesRequest()
        request.id = ["parent"]

        response = namespace.list_namespaces(request)

        self.assertEqual(sorted(response.namespaces), ["parent.child1", "parent.child2"])
        mock_client.get.assert_called_once_with("/namespaces/parent/namespaces")

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_create_namespace(self, mock_rest_client_class):
        """Test creating a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.post.return_value = {"properties": {"key": "value"}}

        namespace = PolarisNamespace(**self.properties)

        request = CreateNamespaceRequest()
        request.id = ["test_namespace"]
        request.properties = {"key": "value"}

        response = namespace.create_namespace(request)

        self.assertEqual(response.properties, {"key": "value"})
        mock_client.post.assert_called_once()

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_create_namespace_already_exists(self, mock_rest_client_class):
        """Test creating a namespace that already exists."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.post.side_effect = RestClientException(
            "Conflict", status_code=409, response_body=""
        )

        namespace = PolarisNamespace(**self.properties)

        request = CreateNamespaceRequest()
        request.id = ["existing_namespace"]

        with self.assertRaises(NamespaceAlreadyExistsException):
            namespace.create_namespace(request)

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_describe_namespace(self, mock_rest_client_class):
        """Test describing a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {"properties": {"key": "value"}}

        namespace = PolarisNamespace(**self.properties)

        request = DescribeNamespaceRequest()
        request.id = ["test_namespace"]

        response = namespace.describe_namespace(request)

        self.assertEqual(response.properties, {"key": "value"})
        mock_client.get.assert_called_once_with("/namespaces/test_namespace")

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_describe_namespace_not_found(self, mock_rest_client_class):
        """Test describing a namespace that doesn't exist."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = RestClientException(
            "Not found", status_code=404, response_body=""
        )

        namespace = PolarisNamespace(**self.properties)

        request = DescribeNamespaceRequest()
        request.id = ["nonexistent"]

        with self.assertRaises(NamespaceNotFoundException):
            namespace.describe_namespace(request)

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_drop_namespace(self, mock_rest_client_class):
        """Test dropping a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        namespace = PolarisNamespace(**self.properties)

        request = DropNamespaceRequest()
        request.id = ["test_namespace"]

        response = namespace.drop_namespace(request)

        self.assertIsNotNone(response)
        mock_client.delete.assert_called_once_with("/namespaces/test_namespace")

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_drop_namespace_not_found(self, mock_rest_client_class):
        """Test dropping a namespace that doesn't exist returns success."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.delete.side_effect = RestClientException(
            "Not found", status_code=404, response_body=""
        )

        namespace = PolarisNamespace(**self.properties)

        request = DropNamespaceRequest()
        request.id = ["nonexistent"]

        response = namespace.drop_namespace(request)

        self.assertIsNotNone(response)

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_list_tables(self, mock_rest_client_class):
        """Test listing tables in a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {
            "identifiers": [
                {"name": "table1"},
                {"name": "table2"},
                {"name": "table3"},
            ]
        }

        namespace = PolarisNamespace(**self.properties)

        request = ListTablesRequest()
        request.id = ["test_namespace"]

        response = namespace.list_tables(request)

        self.assertEqual(sorted(response.tables), ["table1", "table2", "table3"])

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_create_empty_table(self, mock_rest_client_class):
        """Test creating an empty table."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.post.return_value = {}

        namespace = PolarisNamespace(**self.properties)

        request = CreateEmptyTableRequest()
        request.id = ["test_namespace", "test_table"]
        request.location = None

        response = namespace.create_empty_table(request)

        self.assertEqual(
            response.location, "/data/lance/test_namespace/test_table"
        )

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_create_empty_table_with_location(self, mock_rest_client_class):
        """Test creating an empty table with custom location."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.post.return_value = {}

        namespace = PolarisNamespace(**self.properties)

        request = CreateEmptyTableRequest()
        request.id = ["test_namespace", "test_table"]
        request.location = "/custom/path/test_table"

        response = namespace.create_empty_table(request)

        self.assertEqual(response.location, "/custom/path/test_table")

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_create_empty_table_already_exists(self, mock_rest_client_class):
        """Test creating a table that already exists."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.post.side_effect = RestClientException(
            "Conflict", status_code=409, response_body=""
        )

        namespace = PolarisNamespace(**self.properties)

        request = CreateEmptyTableRequest()
        request.id = ["test_namespace", "existing_table"]

        with self.assertRaises(TableAlreadyExistsException):
            namespace.create_empty_table(request)

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_describe_table(self, mock_rest_client_class):
        """Test describing a table."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {
            "table": {
                "format": "lance",
                "base-location": "/data/lance/ns/table",
                "properties": {"key": "value"},
            }
        }

        namespace = PolarisNamespace(**self.properties)

        request = DescribeTableRequest()
        request.id = ["test_namespace", "test_table"]

        response = namespace.describe_table(request)

        self.assertEqual(response.location, "/data/lance/ns/table")
        self.assertEqual(response.storage_options, {"key": "value"})

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_describe_table_not_lance(self, mock_rest_client_class):
        """Test describing a table that is not a Lance table."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {
            "table": {
                "format": "iceberg",
                "base-location": "/data/iceberg/ns/table",
                "properties": {},
            }
        }

        namespace = PolarisNamespace(**self.properties)

        request = DescribeTableRequest()
        request.id = ["test_namespace", "test_table"]

        with self.assertRaises(InvalidInputException):
            namespace.describe_table(request)

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_describe_table_not_found(self, mock_rest_client_class):
        """Test describing a table that doesn't exist."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = RestClientException(
            "Not found", status_code=404, response_body=""
        )

        namespace = PolarisNamespace(**self.properties)

        request = DescribeTableRequest()
        request.id = ["test_namespace", "nonexistent"]

        with self.assertRaises(TableNotFoundException):
            namespace.describe_table(request)

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_deregister_table(self, mock_rest_client_class):
        """Test deregistering a table."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {
            "table": {"base-location": "/data/lance/ns/table"}
        }

        namespace = PolarisNamespace(**self.properties)

        request = DeregisterTableRequest()
        request.id = ["test_namespace", "test_table"]

        response = namespace.deregister_table(request)

        self.assertEqual(response.location, "/data/lance/ns/table")
        mock_client.delete.assert_called_once()

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_deregister_table_not_found(self, mock_rest_client_class):
        """Test deregistering a table that doesn't exist."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.side_effect = RestClientException(
            "Not found", status_code=404, response_body=""
        )

        namespace = PolarisNamespace(**self.properties)

        request = DeregisterTableRequest()
        request.id = ["test_namespace", "nonexistent"]

        with self.assertRaises(TableNotFoundException):
            namespace.deregister_table(request)

    @patch("lance_namespace_impls.polaris.RestClient")
    def test_close(self, mock_rest_client_class):
        """Test closing the namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        namespace = PolarisNamespace(**self.properties)
        namespace.close()

        mock_client.close.assert_called_once()

    def test_invalid_table_id(self):
        """Test that table operations fail with invalid identifiers."""
        namespace = PolarisNamespace(**self.properties)

        request = CreateEmptyTableRequest()
        request.id = ["only_namespace"]

        with self.assertRaises(InvalidInputException):
            namespace.create_empty_table(request)

    def test_invalid_namespace_id(self):
        """Test that namespace operations fail with empty identifiers."""
        namespace = PolarisNamespace(**self.properties)

        request = CreateNamespaceRequest()
        request.id = []

        with self.assertRaises(InvalidInputException):
            namespace.create_namespace(request)


if __name__ == "__main__":
    unittest.main()
