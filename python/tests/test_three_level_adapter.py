"""Tests for ThreeLevelNamespaceAdapter."""

import pytest
from unittest.mock import Mock, MagicMock

from lance_namespace_impls.three_level_adapter import ThreeLevelNamespaceAdapter
from lance_namespace_urllib3_client.models import (
    ListNamespacesRequest,
    ListNamespacesResponse,
    DescribeNamespaceRequest,
    DescribeNamespaceResponse,
    CreateNamespaceRequest,
    CreateNamespaceResponse,
    DropNamespaceRequest,
    DropNamespaceResponse,
    ListTablesRequest,
    ListTablesResponse,
    DeclareTableRequest,
    DeclareTableResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    DeregisterTableRequest,
    DeregisterTableResponse,
)


@pytest.fixture
def mock_delegate():
    """Create a mock delegate namespace."""
    return Mock()


@pytest.fixture
def adapter(mock_delegate):
    """Create a ThreeLevelNamespaceAdapter with a mock delegate."""
    return ThreeLevelNamespaceAdapter(mock_delegate)


class TestNamespaceId:
    """Tests for namespace_id method."""

    def test_namespace_id_includes_delegate(self, adapter, mock_delegate):
        mock_delegate.namespace_id.return_value = "MockNamespace"
        result = adapter.namespace_id()
        assert "ThreeLevelNamespaceAdapter" in result
        assert "MockNamespace" in result


class TestListNamespaces:
    """Tests for list_namespaces method."""

    def test_list_namespaces_with_root_id(self, adapter, mock_delegate):
        request = ListNamespacesRequest(id=None)
        response = ListNamespacesResponse(namespaces=[])
        mock_delegate.list_namespaces.return_value = response

        result = adapter.list_namespaces(request)

        assert result == response
        mock_delegate.list_namespaces.assert_called_once_with(request)

    def test_list_namespaces_with_one_level(self, adapter, mock_delegate):
        request = ListNamespacesRequest(id=["catalog"])
        response = ListNamespacesResponse(namespaces=[])
        mock_delegate.list_namespaces.return_value = response

        result = adapter.list_namespaces(request)

        assert result == response
        mock_delegate.list_namespaces.assert_called_once_with(request)

    def test_list_namespaces_with_two_levels(self, adapter, mock_delegate):
        request = ListNamespacesRequest(id=["catalog", "database"])
        response = ListNamespacesResponse(namespaces=[])
        mock_delegate.list_namespaces.return_value = response

        result = adapter.list_namespaces(request)

        assert result == response
        mock_delegate.list_namespaces.assert_called_once_with(request)

    def test_list_namespaces_with_three_levels_fails(self, adapter, mock_delegate):
        request = ListNamespacesRequest(id=["catalog", "database", "table"])

        with pytest.raises(ValueError) as exc_info:
            adapter.list_namespaces(request)

        assert "3 levels" in str(exc_info.value)
        assert "at most 2 levels" in str(exc_info.value)
        mock_delegate.list_namespaces.assert_not_called()


class TestCreateNamespace:
    """Tests for create_namespace method."""

    def test_create_namespace_with_valid_id(self, adapter, mock_delegate):
        request = CreateNamespaceRequest(id=["catalog", "database"])
        response = CreateNamespaceResponse()
        mock_delegate.create_namespace.return_value = response

        result = adapter.create_namespace(request)

        assert result == response
        mock_delegate.create_namespace.assert_called_once_with(request)

    def test_create_namespace_with_invalid_id_fails(self, adapter, mock_delegate):
        request = CreateNamespaceRequest(id=["catalog", "database", "extra"])

        with pytest.raises(ValueError) as exc_info:
            adapter.create_namespace(request)

        assert "3 levels" in str(exc_info.value)
        mock_delegate.create_namespace.assert_not_called()


class TestDescribeNamespace:
    """Tests for describe_namespace method."""

    def test_describe_namespace(self, adapter, mock_delegate):
        request = DescribeNamespaceRequest(id=["catalog"])
        response = DescribeNamespaceResponse(properties={})
        mock_delegate.describe_namespace.return_value = response

        result = adapter.describe_namespace(request)

        assert result == response
        mock_delegate.describe_namespace.assert_called_once_with(request)


class TestDropNamespace:
    """Tests for drop_namespace method."""

    def test_drop_namespace(self, adapter, mock_delegate):
        request = DropNamespaceRequest(id=["catalog", "database"])
        response = DropNamespaceResponse()
        mock_delegate.drop_namespace.return_value = response

        result = adapter.drop_namespace(request)

        assert result == response
        mock_delegate.drop_namespace.assert_called_once_with(request)


class TestListTables:
    """Tests for list_tables method."""

    def test_list_tables_with_valid_id(self, adapter, mock_delegate):
        request = ListTablesRequest(id=["catalog", "database"])
        response = ListTablesResponse(tables=[])
        mock_delegate.list_tables.return_value = response

        result = adapter.list_tables(request)

        assert result == response
        mock_delegate.list_tables.assert_called_once_with(request)

    def test_list_tables_with_invalid_id_fails(self, adapter, mock_delegate):
        request = ListTablesRequest(id=["catalog", "database", "table"])

        with pytest.raises(ValueError) as exc_info:
            adapter.list_tables(request)

        assert "3 levels" in str(exc_info.value)
        mock_delegate.list_tables.assert_not_called()


class TestDeclareTable:
    """Tests for declare_table method."""

    def test_declare_table_with_valid_id(self, adapter, mock_delegate):
        request = DeclareTableRequest(id=["catalog", "database", "table"])
        dataset_bytes = b"mock_dataset"
        response = DeclareTableResponse()
        mock_delegate.declare_table.return_value = response

        result = adapter.declare_table(request, dataset_bytes)

        assert result == response
        mock_delegate.declare_table.assert_called_once_with(request, dataset_bytes)

    def test_declare_table_with_two_levels_fails(self, adapter, mock_delegate):
        request = DeclareTableRequest(id=["catalog", "database"])
        dataset_bytes = b"mock_dataset"

        with pytest.raises(ValueError) as exc_info:
            adapter.declare_table(request, dataset_bytes)

        assert "exactly 3 levels" in str(exc_info.value)
        assert "2 levels" in str(exc_info.value)
        mock_delegate.declare_table.assert_not_called()

    def test_declare_table_with_four_levels_fails(self, adapter, mock_delegate):
        request = DeclareTableRequest(id=["catalog", "database", "table", "extra"])
        dataset_bytes = b"mock_dataset"

        with pytest.raises(ValueError) as exc_info:
            adapter.declare_table(request, dataset_bytes)

        assert "exactly 3 levels" in str(exc_info.value)
        assert "4 levels" in str(exc_info.value)
        mock_delegate.declare_table.assert_not_called()

    def test_declare_table_with_null_id_fails(self, adapter, mock_delegate):
        request = DeclareTableRequest(id=None)
        dataset_bytes = b"mock_dataset"

        with pytest.raises(ValueError) as exc_info:
            adapter.declare_table(request, dataset_bytes)

        assert "exactly 3 levels" in str(exc_info.value)
        mock_delegate.declare_table.assert_not_called()

    def test_declare_table_with_empty_level_fails(self, adapter, mock_delegate):
        request = DeclareTableRequest(id=["catalog", "", "table"])
        dataset_bytes = b"mock_dataset"

        with pytest.raises(ValueError) as exc_info:
            adapter.declare_table(request, dataset_bytes)

        assert "cannot be null or empty" in str(exc_info.value)
        mock_delegate.declare_table.assert_not_called()


class TestDescribeTable:
    """Tests for describe_table method."""

    def test_describe_table(self, adapter, mock_delegate):
        request = DescribeTableRequest(id=["catalog", "database", "table"])
        response = DescribeTableResponse()
        mock_delegate.describe_table.return_value = response

        result = adapter.describe_table(request)

        assert result == response
        mock_delegate.describe_table.assert_called_once_with(request)

    def test_describe_table_with_invalid_id_fails(self, adapter, mock_delegate):
        request = DescribeTableRequest(id=["catalog", "database"])

        with pytest.raises(ValueError) as exc_info:
            adapter.describe_table(request)

        assert "exactly 3 levels" in str(exc_info.value)
        mock_delegate.describe_table.assert_not_called()


class TestDeregisterTable:
    """Tests for deregister_table method."""

    def test_deregister_table(self, adapter, mock_delegate):
        request = DeregisterTableRequest(id=["catalog", "database", "table"])
        response = DeregisterTableResponse()
        mock_delegate.deregister_table.return_value = response

        result = adapter.deregister_table(request)

        assert result == response
        mock_delegate.deregister_table.assert_called_once_with(request)

    def test_deregister_table_with_invalid_id_fails(self, adapter, mock_delegate):
        request = DeregisterTableRequest(id=["catalog", "database"])

        with pytest.raises(ValueError) as exc_info:
            adapter.deregister_table(request)

        assert "exactly 3 levels" in str(exc_info.value)
        mock_delegate.deregister_table.assert_not_called()
