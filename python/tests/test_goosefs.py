"""
Tests for Lance GooseFS Namespace implementation.
"""

import pytest
from unittest.mock import MagicMock, patch

from lance_namespace_impls.goosefs import GooseFSNamespace
from lance_namespace_urllib3_client.models import (
    ListNamespacesRequest,
    DescribeNamespaceRequest,
    CreateNamespaceRequest,
    DropNamespaceRequest,
    ListTablesRequest,
    DescribeTableRequest,
    DeregisterTableRequest,
)


@pytest.fixture
def mock_goosefs_client():
    """Create a mock GooseFS client."""
    with patch("lance_namespace_impls.goosefs.GOOSEFS_CLIENT_AVAILABLE", True):
        with patch(
            "lance_namespace_impls.goosefs.GoosefsMetastoreClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            yield mock_client


@pytest.fixture
def goosefs_namespace(mock_goosefs_client):
    """Create a GooseFSNamespace instance with mocked client."""
    with patch("lance_namespace_impls.goosefs.GOOSEFS_CLIENT_AVAILABLE", True):
        namespace = GooseFSNamespace(
            uri="goosefs://localhost:9220", root="/tmp/lance"
        )
        namespace._client = mock_goosefs_client
        return namespace


class TestGooseFSNamespace:
    """Test cases for GooseFSNamespace."""

    def test_initialization(self):
        """Test namespace initialization."""
        with patch("lance_namespace_impls.goosefs.GOOSEFS_CLIENT_AVAILABLE", True):
            with patch(
                "lance_namespace_impls.goosefs.GoosefsMetastoreClient"
            ) as mock_client:
                namespace = GooseFSNamespace(
                    uri="goosefs://localhost:9220",
                    root="/tmp/lance",
                    timeout=60,
                    max_retries=5,
                )

                assert namespace.host == "localhost"
                assert namespace.port == 9220
                assert namespace.root == "/tmp/lance"
                assert namespace.timeout == 60
                assert namespace.max_retries == 5

                mock_client.assert_not_called()

                _ = namespace.client
                mock_client.from_properties.assert_called_once()

    def test_initialization_without_goosefs_deps(self):
        """Test that initialization fails gracefully without GooseFS dependencies."""
        with patch("lance_namespace_impls.goosefs.GOOSEFS_CLIENT_AVAILABLE", False):
            with pytest.raises(ImportError, match="GooseFS metastore client not found"):
                GooseFSNamespace(uri="goosefs://localhost:9220")

    def test_list_namespaces_root(self, goosefs_namespace, mock_goosefs_client):
        """Test listing namespaces at root level."""
        # The implementation calls client.list_namespaces(grpc_request)
        # which returns a dict like {"namespaces": ["default", "test_db"]}
        mock_goosefs_client.list_namespaces.return_value = {
            "namespaces": ["default", "test_db"]
        }

        request = ListNamespacesRequest()
        response = goosefs_namespace.list_namespaces(request)

        assert "default" in response.namespaces
        assert "test_db" in response.namespaces
        mock_goosefs_client.list_namespaces.assert_called_once()

    def test_list_namespaces_database_level(self, goosefs_namespace, mock_goosefs_client):
        """Test listing namespaces at database level returns empty."""
        mock_goosefs_client.list_namespaces.return_value = {
            "namespaces": []
        }

        request = ListNamespacesRequest(id=["test_db"])
        response = goosefs_namespace.list_namespaces(request)

        assert response.namespaces == []

    def test_describe_namespace_root(self, goosefs_namespace, mock_goosefs_client):
        """Test describing root namespace."""
        # describe_namespace calls client.describe_namespace(grpc_request) -> dict
        mock_goosefs_client.describe_namespace.return_value = {
            "location": "/tmp/lance",
            "host": "localhost",
            "port": "9220",
            "description": "Root namespace for GooseFS",
        }

        request = DescribeNamespaceRequest(id=[])
        response = goosefs_namespace.describe_namespace(request)

        assert response.properties["location"] == "/tmp/lance"
        assert response.properties["host"] == "localhost"
        assert response.properties["port"] == "9220"
        assert "Root namespace" in response.properties["description"]

    def test_describe_namespace_database(self, goosefs_namespace, mock_goosefs_client):
        """Test describing a database namespace."""
        mock_goosefs_client.describe_namespace.return_value = {
            "db_name": "test_db",
            "description": "Test database",
            "location": "/tmp/lance/test_db",
            "owner": "test_user",
            "comment": "Test comment",
            "key": "value",
        }

        request = DescribeNamespaceRequest(id=["test_db"])
        response = goosefs_namespace.describe_namespace(request)

        assert response.properties["db_name"] == "test_db"
        assert response.properties["description"] == "Test database"
        assert response.properties["location"] == "/tmp/lance/test_db"
        assert response.properties["owner"] == "test_user"
        assert response.properties["comment"] == "Test comment"
        assert response.properties["key"] == "value"
        mock_goosefs_client.describe_namespace.assert_called_once()

    def test_create_namespace_database(self, goosefs_namespace, mock_goosefs_client):
        """Test creating a database namespace."""
        mock_goosefs_client.create_namespace.return_value = {
            "properties": {"tables_updated": "table1,table2"},
            "transaction_id": "txn-123",
        }

        request = CreateNamespaceRequest(
            id=["test_db"],
            properties={
                "udb_type": "hive",
                "udb_db_name": "source_db",
                "ignore_sync_errors": "false",
            },
        )
        response = goosefs_namespace.create_namespace(request)

        assert response.properties["tables_updated"] == "table1,table2"
        mock_goosefs_client.create_namespace.assert_called_once()

    def test_drop_namespace_database(self, goosefs_namespace, mock_goosefs_client):
        """Test dropping a database namespace."""
        mock_goosefs_client.drop_namespace.return_value = {
            "properties": {"status": "dropped"},
        }

        request = DropNamespaceRequest(id=["test_db"])
        response = goosefs_namespace.drop_namespace(request)

        assert response.properties["status"] == "dropped"
        mock_goosefs_client.drop_namespace.assert_called_once()

    def test_drop_namespace_not_found(self, goosefs_namespace, mock_goosefs_client):
        """Test dropping a non-existent namespace raises error."""
        from lance_namespace_impls.rest_client import NamespaceNotFoundException

        mock_goosefs_client.drop_namespace.side_effect = Exception("Namespace not found")

        request = DropNamespaceRequest(id=["nonexistent"])
        with pytest.raises(NamespaceNotFoundException, match="does not exist"):
            goosefs_namespace.drop_namespace(request)

    def test_list_tables(self, goosefs_namespace, mock_goosefs_client):
        """Test listing tables in a database."""
        mock_goosefs_client.list_tables.return_value = {
            "tables": ["table1", "table3"]
        }

        request = ListTablesRequest(id=["test_db"])
        response = goosefs_namespace.list_tables(request)

        assert response.tables == ["table1", "table3"]
        mock_goosefs_client.list_tables.assert_called_once()

    def test_describe_table(self, goosefs_namespace, mock_goosefs_client):
        """Test describing a table returns location."""
        mock_goosefs_client.describe_table.return_value = {
            "location": "/tmp/lance/test_db/test_table",
            "storage_options": {"format": "lance"},
            "metadata": {},
        }

        request = DescribeTableRequest(id=["test_db", "test_table"])
        response = goosefs_namespace.describe_table(request)

        assert response.location == "/tmp/lance/test_db/test_table"
        mock_goosefs_client.describe_table.assert_called_once()

    def test_describe_table_not_found(self, goosefs_namespace, mock_goosefs_client):
        """Test that describe_table raises error when table not found."""
        from lance_namespace_impls.rest_client import TableNotFoundException

        mock_goosefs_client.describe_table.side_effect = Exception("Table not found")

        request = DescribeTableRequest(id=["test_db", "test_table"])
        with pytest.raises(TableNotFoundException, match="does not exist"):
            goosefs_namespace.describe_table(request)

    def test_deregister_table(self, goosefs_namespace, mock_goosefs_client):
        """Test deregistering a table."""
        mock_goosefs_client.deregister_table.return_value = {
            "id": ["test_db", "test_table"],
            "location": "/tmp/lance/test_table",
            "properties": {"status": "deregistered"},
            "transaction_id": "txn-dereg-1",
        }

        request = DeregisterTableRequest(id=["test_db", "test_table"])
        response = goosefs_namespace.deregister_table(request)

        assert response.location == "/tmp/lance/test_table"
        mock_goosefs_client.deregister_table.assert_called_once()

    def test_normalize_identifier(self, goosefs_namespace):
        """Test identifier normalization for 2-level hierarchy."""
        assert goosefs_namespace._normalize_identifier(["test_table"]) == (
            "default",
            "test_table",
        )

        assert goosefs_namespace._normalize_identifier(["test_db", "test_table"]) == (
            "test_db",
            "test_table",
        )

        with pytest.raises(ValueError, match="Invalid identifier"):
            goosefs_namespace._normalize_identifier(["a", "b", "c"])

    def test_get_table_location(self, goosefs_namespace):
        """Test getting table location."""
        location = goosefs_namespace._get_table_location("test_db", "test_table")
        assert location == "/tmp/lance/test_db/test_table"

    def test_root_namespace_operations(self, goosefs_namespace, mock_goosefs_client):
        """Test root namespace operations."""
        # Describe root namespace
        mock_goosefs_client.describe_namespace.return_value = {
            "location": "/tmp/lance",
            "host": "localhost",
            "port": "9220",
        }
        request = DescribeNamespaceRequest(id=[])
        response = goosefs_namespace.describe_namespace(request)
        assert response.properties["location"] == "/tmp/lance"

        # List tables at root level
        mock_goosefs_client.list_tables.return_value = {
            "tables": []
        }
        request = ListTablesRequest(id=[])
        response = goosefs_namespace.list_tables(request)
        assert response.tables == []

    def test_pickle_support(self):
        """Test that GooseFSNamespace can be pickled and unpickled."""
        import pickle

        with patch("lance_namespace_impls.goosefs.GOOSEFS_CLIENT_AVAILABLE", True):
            with patch("lance_namespace_impls.goosefs.GoosefsMetastoreClient"):
                namespace = GooseFSNamespace(
                    uri="goosefs://localhost:9220",
                    root="/tmp/lance",
                    timeout=60,
                    max_retries=5,
                )

                pickled = pickle.dumps(namespace)
                assert pickled is not None

                restored = pickle.loads(pickled)
                assert isinstance(restored, GooseFSNamespace)

                assert restored.host == "localhost"
                assert restored.port == 9220
                assert restored.root == "/tmp/lance"
                assert restored.timeout == 60
                assert restored.max_retries == 5

                assert restored._client is None

                with patch(
                    "lance_namespace_impls.goosefs.GoosefsMetastoreClient"
                ) as mock_client:
                    client = restored.client
                    assert client is not None
                    mock_client.from_properties.assert_called_once()

    def test_namespace_exists(self, goosefs_namespace, mock_goosefs_client):
        """Test namespace_exists method."""
        from lance_namespace_urllib3_client.models import NamespaceExistsRequest

        mock_goosefs_client.namespace_exists.return_value = True

        request = NamespaceExistsRequest(id=["test_db"])
        # Should not raise
        goosefs_namespace.namespace_exists(request)

    def test_namespace_exists_not_found(self, goosefs_namespace, mock_goosefs_client):
        """Test namespace_exists raises error when not found."""
        from lance_namespace_urllib3_client.models import NamespaceExistsRequest
        from lance_namespace_impls.rest_client import NamespaceNotFoundException

        mock_goosefs_client.namespace_exists.return_value = False

        request = NamespaceExistsRequest(id=["nonexistent"])
        with pytest.raises(NamespaceNotFoundException, match="does not exist"):
            goosefs_namespace.namespace_exists(request)

    def test_table_exists(self, goosefs_namespace, mock_goosefs_client):
        """Test table_exists method."""
        from lance_namespace_urllib3_client.models import TableExistsRequest

        mock_goosefs_client.table_exists.return_value = True

        request = TableExistsRequest(id=["test_db", "test_table"])
        # Should not raise
        goosefs_namespace.table_exists(request)

    def test_table_exists_not_found(self, goosefs_namespace, mock_goosefs_client):
        """Test table_exists raises error when not found."""
        from lance_namespace_urllib3_client.models import TableExistsRequest
        from lance_namespace_impls.rest_client import TableNotFoundException

        mock_goosefs_client.table_exists.return_value = False

        request = TableExistsRequest(id=["test_db", "nonexistent"])
        with pytest.raises(TableNotFoundException, match="does not exist"):
            goosefs_namespace.table_exists(request)
