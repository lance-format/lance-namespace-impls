"""
Tests for Unity Catalog namespace implementation.
"""

import json
import unittest
from unittest.mock import patch, MagicMock

import pyarrow as pa

from lance_namespace_impls.unity import (
    UnityNamespace,
    UnityNamespaceConfig,
    SchemaInfo,
    TableInfo,
)
from lance_namespace_impls.rest_client import (
    RestClient,
    RestClientException,
)
from lance_namespace_urllib3_client.models import (
    ListNamespacesRequest,
    CreateNamespaceRequest,
    DescribeNamespaceRequest,
    DropNamespaceRequest,
    ListTablesRequest,
    DeclareTableRequest,
    DescribeTableRequest,
)


class TestUnityNamespaceConfig(unittest.TestCase):
    """Test Unity namespace configuration."""

    def test_config_initialization(self):
        """Test configuration initialization with required properties."""
        properties = {
            "unity.endpoint": "https://unity.example.com",
            "unity.root": "/data/lance",
            "unity.auth_token": "test_token",
        }

        config = UnityNamespaceConfig(properties)

        self.assertEqual(config.endpoint, "https://unity.example.com")
        self.assertEqual(config.root, "/data/lance")
        self.assertEqual(config.auth_token, "test_token")

    def test_config_defaults(self):
        """Test configuration with default values."""
        properties = {"unity.endpoint": "https://unity.example.com"}

        config = UnityNamespaceConfig(properties)

        self.assertEqual(config.root, "/tmp/lance")
        self.assertIsNone(config.auth_token)
        self.assertEqual(config.connect_timeout, 10000)
        self.assertEqual(config.read_timeout, 300000)
        self.assertEqual(config.max_retries, 3)

    def test_config_missing_endpoint(self):
        """Test configuration fails without endpoint."""
        properties = {}

        with self.assertRaises(ValueError) as context:
            UnityNamespaceConfig(properties)

        self.assertIn("unity.endpoint", str(context.exception))

    def test_get_full_api_url(self):
        """Test API URL generation."""
        properties = {"unity.endpoint": "https://unity.example.com"}
        config = UnityNamespaceConfig(properties)

        self.assertEqual(
            config.get_full_api_url(), "https://unity.example.com/api/2.1/unity-catalog"
        )

        # Test with endpoint already containing /api/2.1
        properties = {"unity.endpoint": "https://unity.example.com/api/2.1"}
        config = UnityNamespaceConfig(properties)

        self.assertEqual(
            config.get_full_api_url(), "https://unity.example.com/api/2.1/unity-catalog"
        )

        # Test with endpoint already containing full path
        properties = {
            "unity.endpoint": "https://unity.example.com/api/2.1/unity-catalog"
        }
        config = UnityNamespaceConfig(properties)

        self.assertEqual(
            config.get_full_api_url(), "https://unity.example.com/api/2.1/unity-catalog"
        )


class TestRestClient(unittest.TestCase):
    """Test REST client functionality."""

    @patch("lance_namespace_impls.rest_client.urllib3.PoolManager")
    def test_get_request(self, mock_pool_manager):
        """Test GET request."""
        mock_http = MagicMock()
        mock_pool_manager.return_value = mock_http

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.data = b'{"name": "test_schema"}'
        mock_http.request.return_value = mock_response

        client = RestClient("https://api.example.com")
        result = client.get("/schemas/test")

        self.assertEqual(result, {"name": "test_schema"})
        mock_http.request.assert_called_once()

    @patch("lance_namespace_impls.rest_client.urllib3.PoolManager")
    def test_post_request(self, mock_pool_manager):
        """Test POST request."""
        mock_http = MagicMock()
        mock_pool_manager.return_value = mock_http

        mock_response = MagicMock()
        mock_response.status = 201
        mock_response.data = b'{"id": "123"}'
        mock_http.request.return_value = mock_response

        client = RestClient("https://api.example.com")
        result = client.post("/schemas", {"name": "test"})

        self.assertEqual(result, {"id": "123"})
        mock_http.request.assert_called_once()

    @patch("lance_namespace_impls.rest_client.urllib3.PoolManager")
    def test_delete_request(self, mock_pool_manager):
        """Test DELETE request."""
        mock_http = MagicMock()
        mock_pool_manager.return_value = mock_http

        mock_response = MagicMock()
        mock_response.status = 204
        mock_response.data = b""
        mock_http.request.return_value = mock_response

        client = RestClient("https://api.example.com")
        client.delete("/schemas/test")

        mock_http.request.assert_called_once()

    @patch("lance_namespace_impls.rest_client.urllib3.PoolManager")
    def test_error_response(self, mock_pool_manager):
        """Test error response handling."""
        mock_http = MagicMock()
        mock_pool_manager.return_value = mock_http

        mock_response = MagicMock()
        mock_response.status = 404
        mock_response.data = b'{"error": "Not found"}'
        mock_http.request.return_value = mock_response

        client = RestClient("https://api.example.com")

        with self.assertRaises(RestClientException) as context:
            client.get("/schemas/test")

        self.assertEqual(context.exception.status_code, 404)
        self.assertIn("Not found", context.exception.response_body)


class TestUnityNamespace(unittest.TestCase):
    """Test Unity namespace implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.properties = {
            "unity.endpoint": "https://unity.example.com",
            "unity.root": "/data/lance",
        }

    @patch("lance_namespace_impls.unity.RestClient")
    def test_list_namespaces_top_level(self, mock_rest_client_class):
        """Test listing top-level namespaces (catalogs)."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {
            "catalogs": [{"name": "catalog1"}, {"name": "catalog2"}]
        }

        namespace = UnityNamespace(**self.properties)

        request = ListNamespacesRequest()
        request.id = []

        response = namespace.list_namespaces(request)

        self.assertEqual(sorted(response.namespaces), ["catalog1", "catalog2"])
        mock_client.get.assert_called_once_with("/catalogs", params=None)

    @patch("lance_namespace_impls.unity.RestClient")
    def test_list_namespaces_schemas(self, mock_rest_client_class):
        """Test listing schemas in a catalog."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {
            "schemas": [{"name": "schema1"}, {"name": "schema2"}]
        }

        namespace = UnityNamespace(**self.properties)

        request = ListNamespacesRequest()
        request.id = ["test_catalog"]

        response = namespace.list_namespaces(request)

        self.assertEqual(sorted(response.namespaces), ["schema1", "schema2"])
        mock_client.get.assert_called_once_with(
            "/schemas", params={"catalog_name": "test_catalog"}
        )

    @patch("lance_namespace_impls.unity.RestClient")
    def test_create_namespace(self, mock_rest_client_class):
        """Test creating a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_schema_info = SchemaInfo(
            name="test_schema", catalog_name="test_catalog", properties={"key": "value"}
        )
        mock_client.post.return_value = mock_schema_info

        namespace = UnityNamespace(**self.properties)

        request = CreateNamespaceRequest()
        request.id = ["test_catalog", "test_schema"]
        request.properties = {"key": "value"}

        response = namespace.create_namespace(request)

        self.assertEqual(response.properties, {"key": "value"})

    @patch("lance_namespace_impls.unity.RestClient")
    def test_describe_namespace(self, mock_rest_client_class):
        """Test describing a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_schema_info = SchemaInfo(
            name="test_schema", catalog_name="test_catalog", properties={"key": "value"}
        )
        mock_client.get.return_value = mock_schema_info

        namespace = UnityNamespace(**self.properties)

        request = DescribeNamespaceRequest()
        request.id = ["test_catalog", "test_schema"]

        response = namespace.describe_namespace(request)

        self.assertEqual(response.properties, {"key": "value"})
        mock_client.get.assert_called_once()

    @patch("lance_namespace_impls.unity.RestClient")
    def test_drop_namespace(self, mock_rest_client_class):
        """Test dropping a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        namespace = UnityNamespace(**self.properties)

        request = DropNamespaceRequest()
        request.id = ["test_catalog", "test_schema"]

        response = namespace.drop_namespace(request)

        self.assertIsNotNone(response)
        mock_client.delete.assert_called_once()

    @patch("lance_namespace_impls.unity.RestClient")
    def test_list_tables(self, mock_rest_client_class):
        """Test listing tables in a namespace."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_client.get.return_value = {
            "tables": [
                {"name": "table1", "properties": {"table_type": "lance"}},
                {"name": "table2", "properties": {"table_type": "delta"}},
                {"name": "table3", "properties": {"table_type": "lance"}},
            ]
        }

        namespace = UnityNamespace(**self.properties)

        request = ListTablesRequest()
        request.id = ["test_catalog", "test_schema"]

        response = namespace.list_tables(request)

        # Should only return Lance tables
        self.assertEqual(sorted(response.tables), ["table1", "table3"])

    @patch("lance_namespace_impls.unity.RestClient")
    def test_declare_table(self, mock_rest_client_class):
        """Test declaring a table."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_table_info = TableInfo(
            name="test_table",
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_type="EXTERNAL",
            data_source_format="TEXT",
            columns=[],
            storage_location="/data/lance/test_catalog/test_schema/test_table",
            properties={"table_type": "lance"},
        )
        mock_client.post.return_value = mock_table_info

        namespace = UnityNamespace(**self.properties)

        request = DeclareTableRequest()
        request.id = ["test_catalog", "test_schema", "test_table"]

        response = namespace.declare_table(request)

        self.assertEqual(
            response.location, "/data/lance/test_catalog/test_schema/test_table"
        )

        create_table = mock_client.post.call_args.args[1]
        placeholder_column = create_table.columns[0]
        self.assertEqual(placeholder_column.name, "__placeholder_id")
        self.assertEqual(
            placeholder_column.type_json,
            '{"name":"__placeholder_id","type":"long","nullable":true,"metadata":{}}',
        )

    @patch("lance_namespace_impls.unity.RestClient")
    def test_describe_table(self, mock_rest_client_class):
        """Test describing a table."""
        mock_client = MagicMock()
        mock_rest_client_class.return_value = mock_client

        mock_table_info = TableInfo(
            name="test_table",
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_type="EXTERNAL",
            data_source_format="TEXT",
            columns=[],
            storage_location="/data/lance/test_catalog/test_schema/test_table",
            properties={"table_type": "lance"},
        )
        mock_client.get.return_value = mock_table_info

        namespace = UnityNamespace(**self.properties)

        request = DescribeTableRequest()
        request.id = ["test_catalog", "test_schema", "test_table"]

        response = namespace.describe_table(request)

        self.assertEqual(
            response.location, "/data/lance/test_catalog/test_schema/test_table"
        )

    def test_arrow_type_conversion(self):
        """Test Arrow type to Unity type conversion."""
        namespace = UnityNamespace(**self.properties)

        # Test various Arrow types
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type(pa.string()), "STRING"
        )
        self.assertEqual(namespace._convert_arrow_type_to_unity_type(pa.int32()), "INT")
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type(pa.int64()), "LONG"
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type(pa.float32()), "FLOAT"
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type(pa.float64()), "DOUBLE"
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type(pa.bool_()), "BOOLEAN"
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type(pa.date32()), "DATE"
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type(pa.timestamp("us")), "TIMESTAMP"
        )

    def test_arrow_type_to_json_type_conversion(self):
        """Test Arrow type to the type value used in Unity type JSON."""
        namespace = UnityNamespace(**self.properties)

        # Test various Arrow types
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type_json_type(pa.string()),
            "string",
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type_json_type(pa.int32()),
            "integer",
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type_json_type(pa.int64()),
            "long",
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type_json_type(pa.float32()),
            "float",
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type_json_type(pa.float64()),
            "double",
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type_json_type(pa.bool_()),
            "boolean",
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type_json_type(pa.date32()),
            "date",
        )
        self.assertEqual(
            namespace._convert_arrow_type_to_unity_type_json_type(pa.timestamp("us")),
            "timestamp",
        )

    def test_arrow_schema_uses_struct_field_type_json(self):
        """Test Arrow fields use complete StructField JSON."""
        namespace = UnityNamespace(**self.properties)
        schema = pa.schema([pa.field('quoted"name', pa.string(), nullable=False)])

        columns = namespace._convert_arrow_schema_to_unity_columns(schema)

        self.assertEqual(len(columns), 1)
        self.assertEqual(
            json.loads(columns[0].type_json),
            {
                "name": 'quoted"name',
                "type": "string",
                "nullable": False,
                "metadata": {},
            },
        )


if __name__ == "__main__":
    unittest.main()
