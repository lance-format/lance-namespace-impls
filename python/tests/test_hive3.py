"""
Tests for Lance Hive3 Namespace implementation.
"""

import pytest
from unittest.mock import MagicMock, patch

from lance_namespace_impls.hive3 import Hive3Namespace
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
    with patch("lance_namespace_impls.hive3.HIVE_AVAILABLE", True):
        with patch(
            "lance_namespace_impls.hive3.Hive3MetastoreClientWrapper"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            yield mock_client


@pytest.fixture
def hive_namespace(mock_hive_client):
    """Create a Hive3Namespace instance with mocked client."""
    with patch("lance_namespace_impls.hive3.HIVE_AVAILABLE", True):
        namespace = Hive3Namespace(uri="thrift://localhost:9083", root="/tmp/warehouse")
        namespace._client = mock_hive_client
        return namespace


class TestHive3Namespace:
    """Test cases for Hive3Namespace."""

    def test_initialization(self):
        """Test namespace initialization."""
        with patch("lance_namespace_impls.hive3.HIVE_AVAILABLE", True):
            with patch(
                "lance_namespace_impls.hive3.Hive3MetastoreClientWrapper"
            ) as mock_client:
                namespace = Hive3Namespace(
                    uri="thrift://localhost:9083",
                    root="/tmp/warehouse",
                    ugi="user:group1,group2",
                )

                assert namespace.uri == "thrift://localhost:9083"
                assert namespace.root == "/tmp/warehouse"
                assert namespace.ugi == "user:group1,group2"

                mock_client.assert_not_called()

                _ = namespace.client
                mock_client.assert_called_once_with(
                    "thrift://localhost:9083", "user:group1,group2"
                )

    def test_initialization_without_hive_deps(self):
        """Test that initialization fails gracefully without Hive dependencies."""
        with patch("lance_namespace_impls.hive3.HIVE_AVAILABLE", False):
            with pytest.raises(ImportError, match="Hive dependencies not installed"):
                Hive3Namespace(uri="thrift://localhost:9083")

    def test_list_namespaces_root(self, hive_namespace, mock_hive_client):
        """Test listing catalogs at root level."""
        mock_client_instance = MagicMock()
        mock_catalogs = MagicMock()
        mock_catalogs.names = ["hive", "custom_catalog"]
        mock_client_instance.get_catalogs.return_value = mock_catalogs
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = ListNamespacesRequest()
        response = hive_namespace.list_namespaces(request)

        assert "hive" in response.namespaces
        assert "custom_catalog" in response.namespaces

    def test_list_namespaces_catalog_level(self, hive_namespace, mock_hive_client):
        """Test listing databases in a catalog."""
        mock_client_instance = MagicMock()
        mock_client_instance.get_all_databases.return_value = [
            "default",
            "test_db",
            "prod_db",
        ]
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = ListNamespacesRequest(id=["hive"])
        response = hive_namespace.list_namespaces(request)

        assert response.namespaces == ["test_db", "prod_db"]
        mock_client_instance.get_all_databases.assert_called_once()

    def test_describe_namespace_catalog(self, hive_namespace, mock_hive_client):
        """Test describing a catalog namespace."""
        request = DescribeNamespaceRequest(id=["hive"])
        response = hive_namespace.describe_namespace(request)

        assert "Catalog: hive" in response.properties["description"]
        assert "catalog.location.uri" in response.properties

    def test_describe_namespace_database(self, hive_namespace, mock_hive_client):
        """Test describing a database namespace."""
        mock_database = MagicMock()
        mock_database.description = "Test database"
        mock_database.ownerName = "test_user"
        mock_database.locationUri = "/tmp/warehouse/test_db"
        mock_database.parameters = {"key": "value"}

        mock_client_instance = MagicMock()
        mock_client_instance.get_database.return_value = mock_database
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = DescribeNamespaceRequest(id=["hive", "test_db"])
        response = hive_namespace.describe_namespace(request)

        assert response.properties["comment"] == "Test database"
        assert response.properties["owner"] == "test_user"
        assert response.properties["location"] == "/tmp/warehouse/test_db"
        mock_client_instance.get_database.assert_called_once_with("test_db")

    def test_create_namespace_database(self, hive_namespace, mock_hive_client):
        """Test creating a database namespace."""
        mock_client_instance = MagicMock()
        mock_hive_client.__enter__.return_value = mock_client_instance

        with patch("lance_namespace_impls.hive3.HiveDatabase") as mock_hive_db_class:
            mock_hive_db = MagicMock()
            mock_hive_db_class.return_value = mock_hive_db

            request = CreateNamespaceRequest(
                id=["hive", "test_db"],
                properties={"comment": "Test database", "owner": "test_user"},
            )
            hive_namespace.create_namespace(request)

            mock_client_instance.create_database.assert_called_once_with(mock_hive_db)
            assert mock_hive_db.name == "test_db"

    def test_drop_namespace_database(self, hive_namespace, mock_hive_client):
        """Test dropping a database namespace."""
        mock_client_instance = MagicMock()
        mock_client_instance.get_all_tables.return_value = []
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = DropNamespaceRequest(id=["hive", "test_db"])
        hive_namespace.drop_namespace(request)

        mock_client_instance.drop_database.assert_called_once_with(
            "test_db", deleteData=True, cascade=False
        )

    def test_list_tables(self, hive_namespace, mock_hive_client):
        """Test listing tables in a database."""
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

        request = ListTablesRequest(id=["hive", "test_db"])
        response = hive_namespace.list_tables(request)

        assert response.tables == ["table1", "table3"]
        mock_client_instance.get_all_tables.assert_called_once_with("test_db")

    def test_describe_table(self, hive_namespace, mock_hive_client):
        """Test describing a table returns location only.

        Note: load_detailed_metadata=false is the only supported mode, which means
        only location is returned. Other fields (version, schema, etc.) are not populated.
        """
        mock_table = MagicMock()
        mock_table.sd.location = "/tmp/warehouse/test_db/test_table"
        mock_table.parameters = {
            "table_type": "lance",
            "version": "42",
        }

        mock_client_instance = MagicMock()
        mock_client_instance.get_table.return_value = mock_table
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = DescribeTableRequest(id=["hive", "test_db", "test_table"])
        response = hive_namespace.describe_table(request)

        assert response.location == "/tmp/warehouse/test_db/test_table"

        mock_client_instance.get_table.assert_called_once_with("test_db", "test_table")

    def test_deregister_table(self, hive_namespace, mock_hive_client):
        """Test deregistering a table with 3-level identifier."""
        mock_table = MagicMock()
        mock_table.parameters = {"table_type": "lance"}
        mock_table.sd.location = "/tmp/test_table"

        mock_client_instance = MagicMock()
        mock_client_instance.get_table.return_value = mock_table
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = DeregisterTableRequest(id=["hive", "test_db", "test_table"])
        response = hive_namespace.deregister_table(request)

        assert response.location == "/tmp/test_table"
        mock_client_instance.drop_table.assert_called_once_with(
            "test_db", "test_table", deleteData=False
        )

    def test_normalize_identifier(self, hive_namespace):
        """Test identifier normalization for 3-level hierarchy."""
        # Single element defaults to (hive, default, table)
        assert hive_namespace._normalize_identifier(["test_table"]) == (
            "hive",
            "default",
            "test_table",
        )

        # Two elements defaults to (hive, database, table)
        assert hive_namespace._normalize_identifier(["test_db", "test_table"]) == (
            "hive",
            "test_db",
            "test_table",
        )

        # Three elements is (catalog, database, table)
        assert hive_namespace._normalize_identifier(
            ["my_cat", "test_db", "test_table"]
        ) == ("my_cat", "test_db", "test_table")

        # More than three elements should raise an error
        with pytest.raises(ValueError, match="Invalid identifier"):
            hive_namespace._normalize_identifier(["a", "b", "c", "d"])

    def test_get_table_location(self, hive_namespace):
        """Test getting table location for 3-level hierarchy."""
        # Default "hive" catalog uses hive2-compatible path (no catalog in path)
        location = hive_namespace._get_table_location("hive", "test_db", "test_table")
        assert location == "/tmp/warehouse/test_db.db/test_table"

        # Non-default catalog includes catalog in path
        location = hive_namespace._get_table_location("custom", "test_db", "test_table")
        assert location == "/tmp/warehouse/custom/test_db.db/test_table"

    def test_root_namespace_operations(self, hive_namespace):
        """Test root namespace operations."""
        # describe_namespace for root
        request = DescribeNamespaceRequest(id=[])
        response = hive_namespace.describe_namespace(request)
        assert response.properties["location"] == "/tmp/warehouse"

        # list_tables for root should be empty
        request = ListTablesRequest(id=[])
        response = hive_namespace.list_tables(request)
        assert response.tables == []

        # create_namespace for root should fail
        request = CreateNamespaceRequest(id=[])
        with pytest.raises(ValueError, match="Root namespace already exists"):
            hive_namespace.create_namespace(request)

        # drop_namespace for root should fail
        request = DropNamespaceRequest(id=[])
        with pytest.raises(ValueError, match="Cannot drop root namespace"):
            hive_namespace.drop_namespace(request)

    def test_pickle_support(self):
        """Test that Hive3Namespace can be pickled and unpickled."""
        import pickle

        with patch("lance_namespace_impls.hive3.HIVE_AVAILABLE", True):
            with patch("lance_namespace_impls.hive3.Hive3MetastoreClientWrapper"):
                namespace = Hive3Namespace(
                    uri="thrift://localhost:9083",
                    root="/tmp/warehouse",
                    ugi="user:group1,group2",
                    **{
                        "client.pool-size": "5",
                    },
                )

                pickled = pickle.dumps(namespace)
                assert pickled is not None

                restored = pickle.loads(pickled)
                assert isinstance(restored, Hive3Namespace)

                assert restored.uri == "thrift://localhost:9083"
                assert restored.root == "/tmp/warehouse"
                assert restored.ugi == "user:group1,group2"
                assert restored.pool_size == 5

                assert restored._client is None

                with patch(
                    "lance_namespace_impls.hive3.Hive3MetastoreClientWrapper"
                ) as mock_client:
                    client = restored.client
                    assert client is not None
                    mock_client.assert_called_once_with(
                        "thrift://localhost:9083", "user:group1,group2"
                    )
