"""
Tests for Lance Hive2 Namespace implementation.
"""

import pytest
from unittest.mock import MagicMock, patch

from lance_namespace_impls.hive2 import Hive2Namespace
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
def mock_hive_client():
    """Create a mock Hive client."""
    with patch("lance_namespace_impls.hive2.HIVE_AVAILABLE", True):
        with patch(
            "lance_namespace_impls.hive2.HiveMetastoreClientWrapper"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            yield mock_client


@pytest.fixture
def hive_namespace(mock_hive_client):
    """Create a Hive2Namespace instance with mocked client."""
    with patch("lance_namespace_impls.hive2.HIVE_AVAILABLE", True):
        namespace = Hive2Namespace(uri="thrift://localhost:9083", root="/tmp/warehouse")
        namespace._client = mock_hive_client
        return namespace


class TestHive2Namespace:
    """Test cases for Hive2Namespace."""

    def test_initialization(self):
        """Test namespace initialization."""
        with patch("lance_namespace_impls.hive2.HIVE_AVAILABLE", True):
            with patch(
                "lance_namespace_impls.hive2.HiveMetastoreClientWrapper"
            ) as mock_client:
                namespace = Hive2Namespace(
                    uri="thrift://localhost:9083",
                    root="/tmp/warehouse",
                    ugi="user:group1,group2",
                )

                assert namespace.uri == "thrift://localhost:9083"
                assert namespace.root == "/tmp/warehouse"
                assert namespace.ugi == "user:group1,group2"

                # Client should not be initialized yet (lazy loading)
                mock_client.assert_not_called()

                # Access the client property to trigger initialization
                _ = namespace.client
                mock_client.assert_called_once_with(
                    "thrift://localhost:9083", "user:group1,group2"
                )

    def test_initialization_without_hive_deps(self):
        """Test that initialization fails gracefully without Hive dependencies."""
        with patch("lance_namespace_impls.hive2.HIVE_AVAILABLE", False):
            with pytest.raises(ImportError, match="Hive dependencies not installed"):
                Hive2Namespace(uri="thrift://localhost:9083")

    def test_list_namespaces(self, hive_namespace, mock_hive_client):
        """Test listing namespaces (databases)."""
        mock_client_instance = MagicMock()
        mock_client_instance.get_all_databases.return_value = [
            "default",
            "test_db",
            "prod_db",
        ]
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = ListNamespacesRequest()
        response = hive_namespace.list_namespaces(request)

        assert response.namespaces == ["test_db", "prod_db"]
        mock_client_instance.get_all_databases.assert_called_once()

    def test_describe_namespace(self, hive_namespace, mock_hive_client):
        """Test describing a namespace (database)."""
        mock_database = MagicMock()
        mock_database.description = "Test database"
        mock_database.ownerName = "test_user"
        mock_database.locationUri = "/tmp/warehouse/test_db.db"
        mock_database.parameters = {"key": "value"}

        mock_client_instance = MagicMock()
        mock_client_instance.get_database.return_value = mock_database
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = DescribeNamespaceRequest(id=["test_db"])
        response = hive_namespace.describe_namespace(request)

        # Response doesn't include id, only properties
        assert response.properties["comment"] == "Test database"
        assert response.properties["owner"] == "test_user"
        assert response.properties["location"] == "/tmp/warehouse/test_db.db"
        assert response.properties["key"] == "value"
        mock_client_instance.get_database.assert_called_once_with("test_db")

    def test_create_namespace(self, hive_namespace, mock_hive_client):
        """Test creating a namespace (database)."""
        mock_client_instance = MagicMock()
        mock_hive_client.__enter__.return_value = mock_client_instance

        # Mock HiveDatabase class
        with patch("lance_namespace_impls.hive2.HiveDatabase") as mock_hive_db_class:
            mock_hive_db = MagicMock()
            mock_hive_db_class.return_value = mock_hive_db

            request = CreateNamespaceRequest(
                id=["test_db"],
                properties={
                    "comment": "Test database",
                    "owner": "test_user",
                    "location": "/custom/location",
                },
            )
            hive_namespace.create_namespace(request)
            mock_client_instance.create_database.assert_called_once_with(mock_hive_db)

            # Verify the database object properties were set
            assert mock_hive_db.name == "test_db"
            assert mock_hive_db.description == "Test database"
            assert mock_hive_db.ownerName == "test_user"
            assert mock_hive_db.locationUri == "/custom/location"

    def test_drop_namespace(self, hive_namespace, mock_hive_client):
        """Test dropping a namespace (database)."""
        mock_client_instance = MagicMock()
        mock_client_instance.get_all_tables.return_value = []
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = DropNamespaceRequest(id=["test_db"])
        hive_namespace.drop_namespace(request)

        mock_client_instance.get_all_tables.assert_called_once_with("test_db")
        mock_client_instance.drop_database.assert_called_once_with(
            "test_db", deleteData=True, cascade=False
        )

    def test_drop_namespace_not_empty_fails(self, hive_namespace, mock_hive_client):
        """Test that dropping a non-empty namespace fails (only RESTRICT mode is supported)."""
        mock_client_instance = MagicMock()
        mock_client_instance.get_all_tables.return_value = ["table1", "table2"]
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = DropNamespaceRequest(id=["test_db"])

        # Should fail because namespace is not empty and CASCADE is not supported
        with pytest.raises(ValueError, match="is not empty"):
            hive_namespace.drop_namespace(request)

    def test_list_tables(self, hive_namespace, mock_hive_client):
        """Test listing tables in a namespace."""
        mock_table1 = MagicMock()
        mock_table1.parameters = {"table_type": "lance"}

        mock_table2 = MagicMock()
        mock_table2.parameters = {"other_type": "OTHER"}

        mock_table3 = MagicMock()
        mock_table3.parameters = {"table_type": "lance"}

        mock_client_instance = MagicMock()
        mock_client_instance.get_all_tables.return_value = [
            "table1",
            "table2",
            "table3",
        ]
        mock_client_instance.get_table.side_effect = [
            mock_table1,
            mock_table2,
            mock_table3,
        ]
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = ListTablesRequest(id=["test_db"])
        response = hive_namespace.list_tables(request)

        # Should only return Lance table names
        assert response.tables == ["table1", "table3"]
        mock_client_instance.get_all_tables.assert_called_once_with("test_db")

    def test_describe_table(self, hive_namespace, mock_hive_client):
        """Test describing a table returns location and storage_options only.

        Note: load_detailed_metadata=false is the only supported mode, which means
        only location and storage_options are returned. Other fields (version, schema, etc.)
        are not populated.
        """
        mock_table = MagicMock()
        mock_table.sd.location = "/tmp/warehouse/test_db.db/test_table"
        mock_table.owner = "table_owner"
        mock_table.parameters = {
            "table_type": "lance",
            "version": "42",
        }

        mock_client_instance = MagicMock()
        mock_client_instance.get_table.return_value = mock_table
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = DescribeTableRequest(id=["test_db", "test_table"])
        response = hive_namespace.describe_table(request)

        assert response.location == "/tmp/warehouse/test_db.db/test_table"
        # Only location and storage_options are returned (load_detailed_metadata=false)
        assert (
            response.storage_options == {}
        )  # Empty since no storage.* properties configured

        mock_client_instance.get_table.assert_called_once_with("test_db", "test_table")

    def test_deregister_table(self, hive_namespace, mock_hive_client):
        """Test deregistering a table without deleting data."""
        mock_table = MagicMock()
        mock_table.parameters = {"table_type": "lance"}
        mock_table.sd.location = "/tmp/test_table"

        mock_client_instance = MagicMock()
        mock_client_instance.get_table.return_value = mock_table
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = DeregisterTableRequest(id=["test_db", "test_table"])
        response = hive_namespace.deregister_table(request)

        assert response.location == "/tmp/test_table"
        mock_client_instance.drop_table.assert_called_once_with(
            "test_db", "test_table", deleteData=False
        )

    def test_normalize_identifier(self, hive_namespace):
        """Test identifier normalization."""
        # Single element should default to "default" database
        assert hive_namespace._normalize_identifier(["test_table"]) == (
            "default",
            "test_table",
        )

        # Two elements should be (database, table)
        assert hive_namespace._normalize_identifier(["test_db", "test_table"]) == (
            "test_db",
            "test_table",
        )

        # More than two elements should raise an error
        with pytest.raises(ValueError, match="Invalid identifier"):
            hive_namespace._normalize_identifier(["a", "b", "c"])

    def test_get_table_location(self, hive_namespace):
        """Test getting table location."""
        location = hive_namespace._get_table_location("test_db", "test_table")
        assert location == "/tmp/warehouse/test_db.db/test_table"

    def test_root_namespace_operations(self, hive_namespace):
        """Test root namespace operations."""
        # Test describe_namespace for root
        request = DescribeNamespaceRequest(id=[])
        response = hive_namespace.describe_namespace(request)
        assert response.properties["location"] == "/tmp/warehouse"
        assert "Root namespace" in response.properties["description"]

        # Test list_tables for root (should be empty)
        request = ListTablesRequest(id=[])
        response = hive_namespace.list_tables(request)
        assert response.tables == []

        # Test create_namespace for root (should fail)
        request = CreateNamespaceRequest(id=[])
        with pytest.raises(ValueError, match="Root namespace already exists"):
            hive_namespace.create_namespace(request)

        # Test drop_namespace for root (should fail)
        request = DropNamespaceRequest(id=[])
        with pytest.raises(ValueError, match="Cannot drop root namespace"):
            hive_namespace.drop_namespace(request)

    def test_pickle_support(self):
        """Test that Hive2Namespace can be pickled and unpickled for Ray compatibility."""
        import pickle

        with patch("lance_namespace_impls.hive2.HIVE_AVAILABLE", True):
            with patch("lance_namespace_impls.hive2.HiveMetastoreClientWrapper"):
                namespace = Hive2Namespace(
                    uri="thrift://localhost:9083",
                    root="/tmp/warehouse",
                    ugi="user:group1,group2",
                    **{
                        "client.pool-size": "5",
                        "storage.access_key_id": "test-key",
                        "storage.secret_access_key": "test-secret",
                    },
                )

                pickled = pickle.dumps(namespace)
                assert pickled is not None

                restored = pickle.loads(pickled)
                assert isinstance(restored, Hive2Namespace)

                assert restored.uri == "thrift://localhost:9083"
                assert restored.root == "/tmp/warehouse"
                assert restored.ugi == "user:group1,group2"
                assert restored.pool_size == 5
                assert restored.storage_properties["access_key_id"] == "test-key"
                assert restored.storage_properties["secret_access_key"] == "test-secret"

                assert restored._client is None

                with patch(
                    "lance_namespace_impls.hive2.HiveMetastoreClientWrapper"
                ) as mock_client:
                    client = restored.client
                    assert client is not None
                    assert restored._client is not None
                    mock_client.assert_called_once_with(
                        "thrift://localhost:9083", "user:group1,group2"
                    )
