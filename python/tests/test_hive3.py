"""
Tests for Lance Hive3 Namespace implementation.
"""
import os
import pytest
import tempfile
from unittest.mock import Mock, MagicMock, patch
import pyarrow as pa

from lance_namespace_impls.hive3 import Hive3Namespace
from lance_namespace_urllib3_client.models import (
    ListNamespacesRequest,
    DescribeNamespaceRequest,
    CreateNamespaceRequest,
    DropNamespaceRequest,
    NamespaceExistsRequest,
    ListTablesRequest,
    DescribeTableRequest,
    RegisterTableRequest,
    DeregisterTableRequest,
    TableExistsRequest,
    DropTableRequest,
)


@pytest.fixture
def mock_hive_client():
    """Create a mock Hive client."""
    with patch("lance_namespace_impls.hive3.HIVE_AVAILABLE", True):
        with patch("lance_namespace_impls.hive3.Hive3MetastoreClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            yield mock_client


@pytest.fixture
def hive_namespace(mock_hive_client):
    """Create a Hive3Namespace instance with mocked client."""
    with patch("lance_namespace_impls.hive3.HIVE_AVAILABLE", True):
        namespace = Hive3Namespace(
            uri="thrift://localhost:9083",
            root="/tmp/warehouse"
        )
        namespace._client = mock_hive_client
        return namespace


class TestHive3Namespace:
    """Test cases for Hive3Namespace."""

    def test_initialization(self):
        """Test namespace initialization."""
        with patch("lance_namespace_impls.hive3.HIVE_AVAILABLE", True):
            with patch("lance_namespace_impls.hive3.Hive3MetastoreClient") as mock_client:
                namespace = Hive3Namespace(
                    uri="thrift://localhost:9083",
                    root="/tmp/warehouse",
                    ugi="user:group1,group2"
                )

                assert namespace.uri == "thrift://localhost:9083"
                assert namespace.root == "/tmp/warehouse"
                assert namespace.ugi == "user:group1,group2"

                mock_client.assert_not_called()

                _ = namespace.client
                mock_client.assert_called_once_with("thrift://localhost:9083", "user:group1,group2")

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
        mock_client_instance.get_all_databases.return_value = ["default", "test_db", "prod_db"]
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
                properties={"comment": "Test database", "owner": "test_user"}
            )
            response = hive_namespace.create_namespace(request)

            mock_client_instance.create_database.assert_called_once_with(mock_hive_db)
            assert mock_hive_db.name == "test_db"

    def test_drop_namespace_database(self, hive_namespace, mock_hive_client):
        """Test dropping a database namespace."""
        mock_client_instance = MagicMock()
        mock_client_instance.get_all_tables.return_value = []
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = DropNamespaceRequest(id=["hive", "test_db"])
        response = hive_namespace.drop_namespace(request)

        mock_client_instance.drop_database.assert_called_once_with("test_db", deleteData=True, cascade=False)

    def test_namespace_exists_database(self, hive_namespace, mock_hive_client):
        """Test checking if a database namespace exists."""
        mock_client_instance = MagicMock()
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = NamespaceExistsRequest(id=["hive", "test_db"])
        hive_namespace.namespace_exists(request)

        mock_client_instance.get_database.assert_called_once_with("test_db")

    def test_list_tables(self, hive_namespace, mock_hive_client):
        """Test listing tables in a database."""
        mock_table1 = MagicMock()
        mock_table1.parameters = {"table_type": "lance"}

        mock_table2 = MagicMock()
        mock_table2.parameters = {"other_type": "OTHER"}

        mock_table3 = MagicMock()
        mock_table3.parameters = {"table_type": "lance"}

        mock_client_instance = MagicMock()
        mock_client_instance.get_all_tables.return_value = ["table1", "table2", "table3"]
        mock_client_instance.get_table.side_effect = [mock_table1, mock_table2, mock_table3]
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = ListTablesRequest(id=["hive", "test_db"])
        response = hive_namespace.list_tables(request)

        assert response.tables == ["table1", "table3"]
        mock_client_instance.get_all_tables.assert_called_once_with("test_db")

    def test_describe_table(self, hive_namespace, mock_hive_client):
        """Test describing a table with 3-level identifier."""
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
        assert response.version == 42

        mock_client_instance.get_table.assert_called_once_with("test_db", "test_table")

    def test_register_table(self, hive_namespace, mock_hive_client):
        """Test registering a Lance table with 3-level identifier."""
        with tempfile.TemporaryDirectory() as tmpdir:
            table_path = os.path.join(tmpdir, "test_table")

            data = pa.table({
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"]
            })

            with patch("lance_namespace_impls.hive3.lance.dataset") as mock_dataset_func:
                mock_dataset = MagicMock()
                mock_dataset.schema = data.schema
                mock_dataset.version = 1
                mock_dataset_func.return_value = mock_dataset

                mock_client_instance = MagicMock()
                mock_hive_client.__enter__.return_value = mock_client_instance

                with patch("lance_namespace_impls.hive3.HiveTable") as mock_hive_table_class, \
                     patch("lance_namespace_impls.hive3.StorageDescriptor") as mock_sd_class, \
                     patch("lance_namespace_impls.hive3.SerDeInfo") as mock_serde_class, \
                     patch("lance_namespace_impls.hive3.FieldSchema") as mock_field_class:

                    mock_hive_table = MagicMock()
                    mock_hive_table_class.return_value = mock_hive_table
                    mock_sd = MagicMock()
                    mock_sd_class.return_value = mock_sd
                    mock_serde = MagicMock()
                    mock_serde_class.return_value = mock_serde
                    mock_field_class.return_value = MagicMock()

                    request = RegisterTableRequest(
                        id=["hive", "test_db", "test_table"],
                        location=table_path,
                        properties={"owner": "test_user"}
                    )
                    response = hive_namespace.register_table(request)

                    assert response.location == table_path
                    mock_client_instance.create_table.assert_called_once_with(mock_hive_table)
                    assert mock_hive_table.dbName == "test_db"
                    assert mock_hive_table.tableName == "test_table"

    def test_table_exists(self, hive_namespace, mock_hive_client):
        """Test checking if a table exists with 3-level identifier."""
        mock_table = MagicMock()
        mock_table.parameters = {"table_type": "lance"}

        mock_client_instance = MagicMock()
        mock_client_instance.get_table.return_value = mock_table
        mock_hive_client.__enter__.return_value = mock_client_instance

        request = TableExistsRequest(id=["hive", "test_db", "test_table"])
        hive_namespace.table_exists(request)

        mock_client_instance.get_table.assert_called_once_with("test_db", "test_table")

    def test_drop_table_not_supported(self, hive_namespace, mock_hive_client):
        """Test that drop_table raises NotImplementedError."""
        request = DropTableRequest(id=["hive", "test_db", "test_table"])

        with pytest.raises(NotImplementedError, match="drop_table is not supported"):
            hive_namespace.drop_table(request)

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
        assert hive_namespace._normalize_identifier(["test_table"]) == ("hive", "default", "test_table")

        # Two elements defaults to (hive, database, table)
        assert hive_namespace._normalize_identifier(["test_db", "test_table"]) == ("hive", "test_db", "test_table")

        # Three elements is (catalog, database, table)
        assert hive_namespace._normalize_identifier(["my_cat", "test_db", "test_table"]) == ("my_cat", "test_db", "test_table")

        # More than three elements should raise an error
        with pytest.raises(ValueError, match="Invalid identifier"):
            hive_namespace._normalize_identifier(["a", "b", "c", "d"])

    def test_get_table_location(self, hive_namespace):
        """Test getting table location for 3-level hierarchy."""
        location = hive_namespace._get_table_location("hive", "test_db", "test_table")
        assert location == "/tmp/warehouse/test_db/test_table.lance"

    def test_root_namespace_operations(self, hive_namespace):
        """Test root namespace operations."""
        # namespace_exists for root should not raise
        request = NamespaceExistsRequest(id=[])
        hive_namespace.namespace_exists(request)

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
            with patch("lance_namespace_impls.hive3.Hive3MetastoreClient"):
                namespace = Hive3Namespace(
                    uri="thrift://localhost:9083",
                    root="/tmp/warehouse",
                    ugi="user:group1,group2",
                    **{
                        "client.pool-size": "5",
                        "storage.access_key_id": "test-key",
                        "storage.secret_access_key": "test-secret"
                    }
                )

                pickled = pickle.dumps(namespace)
                assert pickled is not None

                restored = pickle.loads(pickled)
                assert isinstance(restored, Hive3Namespace)

                assert restored.uri == "thrift://localhost:9083"
                assert restored.root == "/tmp/warehouse"
                assert restored.ugi == "user:group1,group2"
                assert restored.pool_size == 5
                assert restored.storage_properties["access_key_id"] == "test-key"

                assert restored._client is None

                with patch("lance_namespace_impls.hive3.Hive3MetastoreClient") as mock_client:
                    client = restored.client
                    assert client is not None
                    mock_client.assert_called_once_with("thrift://localhost:9083", "user:group1,group2")
