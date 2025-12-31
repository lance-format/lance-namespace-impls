"""
Tests for Lance Glue Namespace implementation.
"""

import pytest
from unittest.mock import MagicMock, patch

from lance_namespace_impls.glue import GlueNamespace, GlueNamespaceConfig
from lance_namespace_urllib3_client.models import (
    ListNamespacesRequest,
    CreateNamespaceRequest,
    DescribeNamespaceRequest,
    DropNamespaceRequest,
    ListTablesRequest,
    DescribeTableRequest,
    DeregisterTableRequest,
)


@pytest.fixture
def mock_boto3():
    """Mock boto3 module."""
    with patch("lance_namespace_impls.glue.boto3") as mock:
        mock.Session.return_value.client.return_value = MagicMock()
        yield mock


@pytest.fixture
def glue_namespace(mock_boto3):
    """Create a GlueNamespace instance with mocked dependencies."""
    properties = {"region": "us-east-1", "catalog_id": "123456789012"}
    namespace = GlueNamespace(**properties)
    return namespace


class TestGlueNamespaceConfig:
    """Test GlueNamespaceConfig class."""

    def test_config_initialization(self):
        """Test configuration initialization."""
        properties = {
            "catalog_id": "123456789012",
            "endpoint": "https://glue.example.com",
            "region": "us-west-2",
            "access_key_id": "AKIAEXAMPLE",
            "secret_access_key": "secret",
            "session_token": "token",
            "profile_name": "default",
            "max_retries": "5",
            "retry_mode": "adaptive",
            "root": "s3://bucket/path",
            "storage.key1": "value1",
            "storage.key2": "value2",
        }

        config = GlueNamespaceConfig(properties)

        assert config.catalog_id == "123456789012"
        assert config.endpoint == "https://glue.example.com"
        assert config.region == "us-west-2"
        assert config.access_key_id == "AKIAEXAMPLE"
        assert config.secret_access_key == "secret"
        assert config.session_token == "token"
        assert config.profile_name == "default"
        assert config.max_retries == 5
        assert config.retry_mode == "adaptive"
        assert config.root == "s3://bucket/path"
        assert config.storage_options == {"key1": "value1", "key2": "value2"}

    def test_config_with_empty_properties(self):
        """Test configuration with empty properties."""
        config = GlueNamespaceConfig({})

        assert config.catalog_id is None
        assert config.endpoint is None
        assert config.region is None
        assert config.max_retries is None
        assert config.root is None
        assert config.storage_options == {}


class TestGlueNamespace:
    """Test GlueNamespace class."""

    def test_initialization_without_boto3(self):
        """Test that initialization fails without boto3."""
        with patch("lance_namespace_impls.glue.HAS_BOTO3", False):
            with pytest.raises(ImportError, match="boto3 is required"):
                GlueNamespace()

    def test_list_namespaces(self, glue_namespace):
        """Test listing namespaces."""
        glue_namespace.glue.get_databases.return_value = {
            "DatabaseList": [
                {"Name": "db1"},
                {"Name": "db2"},
            ]
        }

        request = ListNamespacesRequest()
        response = glue_namespace.list_namespaces(request)

        assert response.namespaces == ["db1", "db2"]
        glue_namespace.glue.get_databases.assert_called_once()

    def test_list_namespaces_with_pagination(self, glue_namespace):
        """Test listing namespaces with pagination."""
        glue_namespace.glue.get_databases.side_effect = [
            {"DatabaseList": [{"Name": "db1"}], "NextToken": "token1"},
            {
                "DatabaseList": [{"Name": "db2"}],
            },
        ]

        request = ListNamespacesRequest()
        response = glue_namespace.list_namespaces(request)

        assert response.namespaces == ["db1", "db2"]
        assert glue_namespace.glue.get_databases.call_count == 2

    def test_list_namespaces_hierarchical_not_supported(self, glue_namespace):
        """Test that hierarchical namespaces are not supported."""
        request = ListNamespacesRequest(id=["parent"])
        response = glue_namespace.list_namespaces(request)

        assert response.namespaces == []
        glue_namespace.glue.get_databases.assert_not_called()

    def test_list_namespaces_root(self, glue_namespace):
        """Test listing namespaces at root level."""
        glue_namespace.glue.get_databases.return_value = {
            "DatabaseList": [
                {"Name": "db1"},
                {"Name": "db2"},
            ]
        }

        # Empty id means root namespace
        request = ListNamespacesRequest(id=[])
        response = glue_namespace.list_namespaces(request)

        assert response.namespaces == ["db1", "db2"]
        glue_namespace.glue.get_databases.assert_called_once()

    def test_create_namespace(self, glue_namespace):
        """Test creating a namespace."""
        request = CreateNamespaceRequest(
            id=["test_db"],
            properties={"description": "Test database", "location": "s3://bucket/path"},
        )

        glue_namespace.create_namespace(request)

        glue_namespace.glue.create_database.assert_called_once()
        call_args = glue_namespace.glue.create_database.call_args
        assert call_args[1]["DatabaseInput"]["Name"] == "test_db"
        assert call_args[1]["DatabaseInput"]["Description"] == "Test database"
        assert call_args[1]["DatabaseInput"]["LocationUri"] == "s3://bucket/path"

    def test_create_namespace_root(self, glue_namespace):
        """Test creating root namespace fails."""
        request = CreateNamespaceRequest(id=[])

        with pytest.raises(RuntimeError, match="Root namespace already exists"):
            glue_namespace.create_namespace(request)

        glue_namespace.glue.create_database.assert_not_called()

    def test_create_namespace_already_exists(self, glue_namespace):
        """Test creating a namespace that already exists."""

        # Create a custom exception with the right name
        class AlreadyExistsException(Exception):
            pass

        glue_namespace.glue.exceptions.AlreadyExistsException = AlreadyExistsException
        glue_namespace.glue.create_database.side_effect = AlreadyExistsException(
            "Already exists"
        )

        request = CreateNamespaceRequest(id=["test_db"])

        with pytest.raises(RuntimeError, match="Namespace already exists"):
            glue_namespace.create_namespace(request)

    def test_describe_namespace_root(self, glue_namespace):
        """Test describing root namespace."""
        request = DescribeNamespaceRequest(id=[])
        response = glue_namespace.describe_namespace(request)

        assert response.properties["description"] == "Root Glue catalog namespace"
        glue_namespace.glue.get_database.assert_not_called()

    def test_describe_namespace(self, glue_namespace):
        """Test describing a namespace."""
        glue_namespace.glue.get_database.return_value = {
            "Database": {
                "Name": "test_db",
                "Description": "Test database",
                "LocationUri": "s3://bucket/path",
                "Parameters": {"key": "value"},
            }
        }

        request = DescribeNamespaceRequest(id=["test_db"])
        response = glue_namespace.describe_namespace(request)

        assert response.properties["description"] == "Test database"
        assert response.properties["location"] == "s3://bucket/path"
        assert response.properties["key"] == "value"

    def test_drop_namespace_root(self, glue_namespace):
        """Test dropping root namespace fails."""
        request = DropNamespaceRequest(id=[])

        with pytest.raises(RuntimeError, match="Cannot drop root namespace"):
            glue_namespace.drop_namespace(request)

        glue_namespace.glue.get_tables.assert_not_called()
        glue_namespace.glue.delete_database.assert_not_called()

    def test_drop_namespace(self, glue_namespace):
        """Test dropping an empty namespace."""
        glue_namespace.glue.get_tables.return_value = {"TableList": []}

        request = DropNamespaceRequest(id=["test_db"])
        glue_namespace.drop_namespace(request)

        glue_namespace.glue.get_tables.assert_called_once_with(DatabaseName="test_db")
        glue_namespace.glue.delete_database.assert_called_once_with(Name="test_db")

    def test_drop_namespace_not_empty(self, glue_namespace):
        """Test dropping a non-empty namespace."""
        glue_namespace.glue.get_tables.return_value = {
            "TableList": [{"Name": "table1"}]
        }

        request = DropNamespaceRequest(id=["test_db"])

        with pytest.raises(RuntimeError, match="Cannot drop non-empty namespace"):
            glue_namespace.drop_namespace(request)

    def test_list_tables_root(self, glue_namespace):
        """Test listing tables at root namespace returns empty."""
        request = ListTablesRequest(id=[])
        response = glue_namespace.list_tables(request)

        assert response.tables == []
        glue_namespace.glue.get_tables.assert_not_called()

    def test_list_tables(self, glue_namespace):
        """Test listing tables in a namespace."""
        glue_namespace.glue.get_tables.return_value = {
            "TableList": [
                {"Name": "table1", "Parameters": {"table_type": "LANCE"}},
                {"Name": "table2", "Parameters": {"table_type": "LANCE"}},
                {
                    "Name": "table3",
                    "Parameters": {"table_type": "HIVE"},
                },  # Not a Lance table
            ]
        }

        request = ListTablesRequest(id=["test_db"])
        response = glue_namespace.list_tables(request)

        assert response.tables == ["table1", "table2"]
        glue_namespace.glue.get_tables.assert_called_once_with(DatabaseName="test_db")

    def test_deregister_table(self, glue_namespace):
        """Test deregistering a table (only removes from Glue, keeps Lance dataset)."""
        request = DeregisterTableRequest(id=["test_db", "test_table"])
        glue_namespace.deregister_table(request)

        glue_namespace.glue.delete_table.assert_called_once_with(
            DatabaseName="test_db", Name="test_table"
        )

    def test_describe_table(self, glue_namespace):
        """Test describing a table."""
        glue_namespace.glue.get_table.return_value = {
            "Table": {
                "Name": "test_table",
                "Parameters": {"table_type": "LANCE"},
                "StorageDescriptor": {"Location": "s3://bucket/table.lance"},
            }
        }

        request = DescribeTableRequest(id=["test_db", "test_table"])
        response = glue_namespace.describe_table(request)

        assert response.location == "s3://bucket/table.lance"

    def test_describe_table_not_lance(self, glue_namespace):
        """Test describing a non-Lance table."""
        glue_namespace.glue.get_table.return_value = {
            "Table": {
                "Name": "test_table",
                "Parameters": {"table_type": "HIVE"},
                "StorageDescriptor": {"Location": "s3://bucket/table"},
            }
        }

        request = DescribeTableRequest(id=["test_db", "test_table"])

        with pytest.raises(RuntimeError, match="Table is not a Lance table"):
            glue_namespace.describe_table(request)

    def test_parse_table_identifier(self, glue_namespace):
        """Test parsing table identifier."""
        db, table = glue_namespace._parse_table_identifier(["db", "table"])
        assert db == "db"
        assert table == "table"

        with pytest.raises(ValueError, match="exactly 2 parts"):
            glue_namespace._parse_table_identifier(["db"])

        with pytest.raises(ValueError, match="exactly 2 parts"):
            glue_namespace._parse_table_identifier(["db", "schema", "table"])

    def test_is_lance_table(self, glue_namespace):
        """Test checking if a Glue table is a Lance table."""
        lance_table = {"Parameters": {"table_type": "LANCE"}}
        assert glue_namespace._is_lance_table(lance_table) is True

        lance_table_lower = {"Parameters": {"table_type": "lance"}}
        assert glue_namespace._is_lance_table(lance_table_lower) is True

        hive_table = {"Parameters": {"table_type": "HIVE"}}
        assert glue_namespace._is_lance_table(hive_table) is False

        no_params = {}
        assert glue_namespace._is_lance_table(no_params) is False

    def test_pickle_support(self, mock_boto3):
        """Test that GlueNamespace can be pickled and unpickled for Ray compatibility."""
        import pickle

        # Create a GlueNamespace instance
        properties = {
            "region": "us-east-1",
            "catalog_id": "123456789012",
            "endpoint": "https://glue.example.com",
            "storage.access_key_id": "test-key",
            "storage.secret_access_key": "test-secret",
        }
        namespace = GlueNamespace(**properties)

        # Test pickling
        pickled = pickle.dumps(namespace)
        assert pickled is not None

        # Test unpickling
        restored = pickle.loads(pickled)
        assert isinstance(restored, GlueNamespace)

        # Verify configuration is preserved
        assert restored.config.region == "us-east-1"
        assert restored.config.catalog_id == "123456789012"
        assert restored.config.endpoint == "https://glue.example.com"
        assert restored.config.storage_options["access_key_id"] == "test-key"
        assert restored.config.storage_options["secret_access_key"] == "test-secret"

        # Verify glue client is None after unpickling (will be lazily initialized)
        assert restored._glue is None

        # Test that glue client can be re-initialized after unpickling
        # This will create a new mock client when accessed
        client = restored.glue
        assert client is not None
        assert restored._glue is not None
