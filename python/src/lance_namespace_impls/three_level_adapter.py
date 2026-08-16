"""
Three-level namespace adapter that enforces exactly 3 levels for tables and 2 levels for namespaces.

This adapter wraps any LanceNamespace implementation and validates that:
- Table identifiers have exactly 3 levels: [catalog, database, table]
- Namespace identifiers have at most 2 levels: [catalog] or [catalog, database]
- Operations on non-conforming identifiers are rejected

This is useful for namespace implementations that need to enforce a strict 3-level hierarchy
(e.g., Unity Catalog, Gravitino) while allowing the underlying implementation to be more flexible.

Example usage:
    >>> from lance_namespace import connect
    >>> from lance_namespace_impls.three_level_adapter import ThreeLevelNamespaceAdapter
    >>>
    >>> # Wrap any namespace implementation
    >>> underlying = connect("hive3", {"uri": "thrift://localhost:9083"})
    >>> enforced = ThreeLevelNamespaceAdapter(underlying)
    >>>
    >>> # Now all operations enforce 3-level table identifiers
    >>> enforced.declare_table(DeclareTableRequest(
    ...     id=["catalog", "database", "table"],  # Must be exactly 3 levels
    ...     var_schema=schema
    ... ), dataset)
"""

from typing import Optional, List

from lance.namespace import LanceNamespace
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


class ThreeLevelNamespaceAdapter(LanceNamespace):
    """Adapter that enforces exactly 3 levels for table identifiers and 2 levels for namespace identifiers.

    This adapter wraps any LanceNamespace implementation and validates identifier levels
    before delegating to the underlying implementation.
    """

    def __init__(self, delegate: LanceNamespace):
        """Initialize the adapter with an underlying namespace implementation.

        Args:
            delegate: The underlying LanceNamespace implementation to wrap
        """
        self.delegate = delegate

    def namespace_id(self) -> str:
        """Return a human-readable unique identifier for this namespace instance."""
        return f"ThreeLevelNamespaceAdapter {{ delegate: {self.delegate.namespace_id()} }}"

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        """List namespaces, enforcing at most 2 levels."""
        self._validate_namespace_id(request.id, "list")
        return self.delegate.list_namespaces(request)

    def create_namespace(
        self, request: CreateNamespaceRequest
    ) -> CreateNamespaceResponse:
        """Create a namespace, enforcing at most 2 levels."""
        self._validate_namespace_id(request.id, "create")
        return self.delegate.create_namespace(request)

    def describe_namespace(
        self, request: DescribeNamespaceRequest
    ) -> DescribeNamespaceResponse:
        """Describe a namespace, enforcing at most 2 levels."""
        self._validate_namespace_id(request.id, "describe")
        return self.delegate.describe_namespace(request)

    def drop_namespace(self, request: DropNamespaceRequest) -> DropNamespaceResponse:
        """Drop a namespace, enforcing at most 2 levels."""
        self._validate_namespace_id(request.id, "drop")
        return self.delegate.drop_namespace(request)

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        """List tables in a namespace, enforcing at most 2 levels for the namespace."""
        self._validate_namespace_id(request.id, "list tables in")
        return self.delegate.list_tables(request)

    def declare_table(
        self, request: DeclareTableRequest, dataset_bytes: bytes
    ) -> DeclareTableResponse:
        """Declare a table, enforcing exactly 3 levels."""
        self._validate_table_id(request.id)
        return self.delegate.declare_table(request, dataset_bytes)

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        """Describe a table, enforcing exactly 3 levels."""
        self._validate_table_id(request.id)
        return self.delegate.describe_table(request)

    def deregister_table(
        self, request: DeregisterTableRequest
    ) -> DeregisterTableResponse:
        """Deregister a table, enforcing exactly 3 levels."""
        self._validate_table_id(request.id)
        return self.delegate.deregister_table(request)

    def _validate_namespace_id(self, id: Optional[List[str]], operation: str) -> None:
        """Validate that a namespace identifier has at most 2 levels.

        Args:
            id: The namespace identifier to validate
            operation: The operation being performed (for error messages)

        Raises:
            ValueError: If the identifier has more than 2 levels
        """
        if id is None:
            return  # Root namespace is allowed

        levels = len(id)
        if levels > 2:
            raise ValueError(
                f"Cannot {operation} namespace with {levels} levels. "
                f"Expected at most 2 levels (catalog.database), but got: {'.'.join(id)}"
            )

    def _validate_table_id(self, id: Optional[List[str]]) -> None:
        """Validate that a table identifier has exactly 3 levels.

        Args:
            id: The table identifier to validate

        Raises:
            ValueError: If the identifier does not have exactly 3 levels
        """
        if id is None or len(id) != 3:
            id_str = ".".join(id) if id else "null"
            levels = len(id) if id else 0
            raise ValueError(
                f"Table identifier must have exactly 3 levels (catalog.database.table), "
                f"but got {levels} levels: {id_str}"
            )

        # Validate that no level is null or empty
        for i, level in enumerate(id):
            if not level:
                raise ValueError(
                    f"Table identifier level {i} cannot be null or empty: {'.'.join(id)}"
                )
