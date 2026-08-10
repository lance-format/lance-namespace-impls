"""
Tests for Gravitino REST namespace implementation.
"""

import unittest
from unittest.mock import MagicMock, Mock, patch

from lance_namespace_impls.gravitino import (
    GravitinoNamespace,
    GravitinoNamespaceConfig,
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
    DeclareTableRequest,
    DescribeTableRequest,
    DeregisterTableRequest,
)


class TestGravitinoNamespaceConfig(unittest.TestCase):
    """Test Gravitino namespace configuration."""

    def test_config_initialization(self):
        """Test configuration initialization with required properties."""
        properties = {
            "endpoint": "http://localhost:9101",
            "auth_token": "test_token",
        }

        config = GravitinoNamespaceConfig(properties)

        self.assertEqual(config.endpoint, "http://localhost:9101")
        self.assertEqual(config.auth_token, "test_token")

    def test_config_defaults(self):
        """Test configuration with default values."""
        properties = {
            "endpoint": "http://localhost:9101",
        }

        config = GravitinoNamespaceConfig(properties)

        self.assertEqual(config.connect_timeout, 10000)
        self.assertEqual(config.read_timeout, 30000)
        self.assertEqual(config.max_retries, 3)
        self.assertIsNone(config.auth_token)

    def test_config_missing_endpoint(self):
        """Test configuration with missing endpoint."""
        properties = {}

        with self.assertRaises(ValueError) as context:
            GravitinoNamespaceConfig(properties)

        self.assertIn("endpoint", str(context.exception))

    def test_get_base_api_url(self):
        """Test base API URL generation."""
        properties = {
            "endpoint": "http://localhost:9101/",
        }

        config = GravitinoNamespaceConfig(properties)
        self.assertEqual(config.get_base_api_url(), "http://localhost:9101/lance/v1")

        properties["endpoint"] = "http://localhost:9101"
        config = GravitinoNamespaceConfig(properties)
        self.assertEqual(config.get_base_api_url(), "http://localhost:9101/lance/v1")


class TestGravitinoNamespace(unittest.TestCase):
    """Test Gravitino namespace implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.properties = {
            "endpoint": "http://localhost:9101",
            "auth_token": "test_token",
        }
        self.namespace = GravitinoNamespace(**self.properties)

    def test_namespace_id(self):
        """Test namespace ID generation."""
        expected = (
            "GravitinoNamespace { endpoint: 'http://localhost:9101' }"
        )
        self.assertEqual(self.namespace.namespace_id(), expected)

    @patch("lance_namespace_impls.gravitino.RestClient")
    def test_list_namespaces_catalogs(self, mock_rest_client_class):
        """Test listing catalogs (top-level namespaces)."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client
        mock_client.get.return_value = {"namespaces": ["catalog1", "catalog2"]}

        namespace = GravitinoNamespace(**self.properties)
        request = ListNamespacesRequest(id=[])
        response = namespace.list_namespaces(request)

        self.assertEqual(response.namespaces, ["catalog1", "catalog2"])
        mock_client.get.assert_called_once_with("/namespace/%2E/list", params={"delimiter": "."})

    @patch("lance_namespace_impls.gravitino.RestClient")
    def test_list_namespaces_schemas(self, mock_rest_client_class):
        """Test listing schemas in a catalog."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client
        mock_client.get.return_value = {"namespaces": ["schema1", "schema2"]}

        namespace = GravitinoNamespace(**self.properties)
        request = ListNamespacesRequest(id=["catalog1"])
        response = namespace.list_namespaces(request)

        self.assertEqual(response.namespaces, ["catalog1.schema1", "catalog1.schema2"])
        mock_client.get.assert_called_once_with("/namespace/catalog1/list")

    @patch("lance_namespace_impls.gravitino.RestClient")
    def test_create_namespace_catalog(self, mock_rest_client_class):
        """Test creating a catalog."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client
        mock_client.post.return_value = {"properties": {"key": "value"}}

        namespace = GravitinoNamespace(**self.properties)
        request = CreateNamespaceRequest(id=["catalog1"], properties={"test": "prop"})
        response = namespace.create_namespace(request)

        self.assertEqual(response.properties, {"key": "value"})
        mock_client.post.assert_called_once()

    @patch("lance_namespace_impls.gravitino.RestClient")
    def test_create_namespace_schema(self, mock_rest_client_class):
        """Test creating a schema in a catalog."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client
        mock_client.post.return_value = {"properties": {"key": "value"}}

        namespace = GravitinoNamespace(**self.properties)
        request = CreateNamespaceRequest(
            id=["catalog1", "schema1"], properties={"test": "prop"}
        )
        response = namespace.create_namespace(request)

        self.assertEqual(response.properties, {"key": "value"})
        mock_client.post.assert_called_once()

    def test_create_namespace_invalid_levels(self):
        """Test creating namespace with invalid number of levels."""
        request = CreateNamespaceRequest(id=["catalog1", "schema1", "invalid"])

        with self.assertRaises(InvalidInputException):
            self.namespace.create_namespace(request)

    @patch("lance_namespace_impls.gravitino.RestClient")
    def test_declare_table(self, mock_rest_client_class):
        """Test declaring a table."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client
        mock_client.post.return_value = {"location": "/path/to/table"}

        namespace = GravitinoNamespace(**self.properties)
        request = DeclareTableRequest(
            id=["catalog1", "schema1", "table1"], location="/path/to/table"
        )
        response = namespace.declare_table(request)

        self.assertEqual(response.location, "/path/to/table")
        mock_client.post.assert_called_once()

    def test_declare_table_invalid_levels(self):
        """Test declaring table with invalid number of levels."""
        request = DeclareTableRequest(id=["catalog1", "schema1"])

        with self.assertRaises(InvalidInputException):
            self.namespace.declare_table(request)

    @patch("lance_namespace_impls.gravitino.RestClient")
    def test_list_tables(self, mock_rest_client_class):
        """Test listing tables in a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client
        mock_client.get.return_value = {"tables": ["table1", "table2"]}

        namespace = GravitinoNamespace(**self.properties)
        request = ListTablesRequest(id=["catalog1", "schema1"])
        response = namespace.list_tables(request)

        self.assertEqual(set(response.tables), {"table1", "table2"})
        mock_client.get.assert_called_once()

    def test_list_tables_invalid_levels(self):
        """Test listing tables with invalid namespace levels."""
        request = ListTablesRequest(id=["catalog1"])

        with self.assertRaises(InvalidInputException):
            self.namespace.list_tables(request)

    @patch("lance_namespace_impls.gravitino.RestClient")
    def test_drop_namespace_with_cascade_behavior(self, mock_rest_client_class):
        """Test that CASCADE behavior is passed to the REST API call."""
        mock_client = Mock()
        mock_rest_client_class.return_value = mock_client
        
        namespace = GravitinoNamespace(**self.properties)
        
        # Test dropping with CASCADE behavior
        request = DropNamespaceRequest(id=["test_catalog"], behavior="CASCADE")
        namespace.drop_namespace(request)
        
        # Verify the REST call was made with the correct payload
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        
        # Check the endpoint
        self.assertEqual(call_args[0][0], "/namespace/test_catalog/drop")
        
        # Check the payload includes both id and behavior
        payload = call_args[0][1]
        self.assertEqual(payload["id"], ["test_catalog"])
        self.assertEqual(payload["behavior"], "CASCADE")

    @patch("lance_namespace_impls.gravitino.RestClient")
    def test_drop_namespace_without_behavior(self, mock_rest_client_class):
        """Test that behavior is not included when not specified."""
        mock_client = Mock()
        mock_rest_client_class.return_value = mock_client
        
        namespace = GravitinoNamespace(**self.properties)
        
        # Test dropping without behavior
        request = DropNamespaceRequest(id=["test_catalog"])
        namespace.drop_namespace(request)
        
        # Verify the REST call was made with the correct payload
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        
        # Check the endpoint
        self.assertEqual(call_args[0][0], "/namespace/test_catalog/drop")
        
        # Check the payload only includes id
        payload = call_args[0][1]
        self.assertEqual(payload["id"], ["test_catalog"])
        self.assertNotIn("behavior", payload)


if __name__ == "__main__":
    unittest.main()