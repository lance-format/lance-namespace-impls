"""
Lance GooseFS Namespace implementation using GooseFS Table Master.

This module provides integration with GooseFS Table Master for managing Lance tables.
GooseFS is a distributed caching system that provides a unified namespace for
accessing data across different storage systems.

The namespace hierarchy in GooseFS is: database > table.

Installation:
    pip install 'lance-namespace-impls[goosefs]'

Usage:
    from lance_namespace_impls import LanceNamespaces

    # Connect to GooseFS Table Master
    namespace = LanceNamespaces.connect("goosefs", {
        "uri": "goosefs://localhost:9220",
        "root": "/my/dir",  # Or "cosn://bucket/prefix"
    })

    # List databases
    from lance_namespace_urllib3_client.models import ListNamespacesRequest
    response = namespace.list_namespaces(ListNamespacesRequest())

    # List tables in a database
    from lance_namespace_urllib3_client.models import ListTablesRequest
    response = namespace.list_tables(ListTablesRequest(id=["my_database"]))

Configuration Properties:
    uri (str): GooseFS master URI (e.g., "goosefs://localhost:9220")
    root (str): Storage root location for Lance tables (e.g., "/my/dir" or "cosn://bucket/prefix")
    connect_timeout (int): Connection timeout in milliseconds (default: 10000)
    read_timeout (int): Read timeout in milliseconds (default: 30000)
    max_retries (int): Maximum number of retry attempts (default: 3)
"""

import logging
import os
from typing import Dict, List, Optional

from lance.namespace import LanceNamespace
from lance_namespace_urllib3_client.models import (
    AlterTableAddColumnsRequest,
    AlterTableAddColumnsResponse,
    AlterTableAlterColumnsRequest,
    AlterTableAlterColumnsResponse,
    AlterTableDropColumnsRequest,
    AlterTableDropColumnsResponse,
    AlterTransactionRequest,
    AlterTransactionResponse,
    AnalyzeTableQueryPlanRequest,
    CountTableRowsRequest,
    CreateEmptyTableRequest,
    CreateEmptyTableResponse,
    CreateNamespaceRequest,
    CreateNamespaceResponse,
    CreateTableIndexRequest,
    CreateTableIndexResponse,
    CreateTableRequest,
    CreateTableResponse,
    CreateTableScalarIndexResponse,
    CreateTableTagRequest,
    CreateTableTagResponse,
    DeclareTableRequest,
    DeclareTableResponse,
    DeleteFromTableRequest,
    DeleteFromTableResponse,
    DeleteTableTagRequest,
    DeleteTableTagResponse,
    DeregisterTableRequest,
    DeregisterTableResponse,
    DescribeNamespaceRequest,
    DescribeNamespaceResponse,
    DescribeTableIndexStatsRequest,
    DescribeTableIndexStatsResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    DescribeTransactionRequest,
    DescribeTransactionResponse,
    DropNamespaceRequest,
    DropNamespaceResponse,
    DropTableIndexRequest,
    DropTableIndexResponse,
    DropTableRequest,
    DropTableResponse,
    ExplainTableQueryPlanRequest,
    GetTableStatsRequest,
    GetTableStatsResponse,
    GetTableTagVersionRequest,
    GetTableTagVersionResponse,
    InsertIntoTableRequest,
    InsertIntoTableResponse,
    ListNamespacesRequest,
    ListNamespacesResponse,
    ListTableIndicesRequest,
    ListTableIndicesResponse,
    ListTableTagsRequest,
    ListTableTagsResponse,
    ListTableVersionsRequest,
    ListTableVersionsResponse,
    ListTablesRequest,
    ListTablesResponse,
    MergeInsertIntoTableRequest,
    MergeInsertIntoTableResponse,
    NamespaceExistsRequest,
    QueryTableRequest,
    RegisterTableRequest,
    RegisterTableResponse,
    RenameTableRequest,
    RenameTableResponse,
    RestoreTableRequest,
    RestoreTableResponse,
    TableExistsRequest,
    UpdateTableRequest,
    UpdateTableResponse,
    UpdateTableSchemaMetadataRequest,
    UpdateTableSchemaMetadataResponse,
    UpdateTableTagRequest,
    UpdateTableTagResponse,
)

# The following models may not exist in older versions of lance_namespace_urllib3_client,
# so we use try/except for conditional imports
try:
    from lance_namespace_urllib3_client.models import (
        BatchCommitTablesRequest,
        BatchCommitTablesResponse,
        BatchCreateTableVersionsRequest,
        BatchCreateTableVersionsResponse,
        BatchDeleteTableVersionsRequest,
        BatchDeleteTableVersionsResponse,
        CommitTableOperation,
        CommitTableResult,
        CreateTableVersionEntry,
        CreateTableVersionRequest,
        CreateTableVersionResponse,
        DescribeTableVersionRequest,
        DescribeTableVersionResponse,
    )
except ImportError:
    BatchCommitTablesRequest = None
    BatchCommitTablesResponse = None
    BatchCreateTableVersionsRequest = None
    BatchCreateTableVersionsResponse = None
    BatchDeleteTableVersionsRequest = None
    BatchDeleteTableVersionsResponse = None
    CommitTableOperation = None
    CommitTableResult = None
    CreateTableVersionEntry = None
    CreateTableVersionRequest = None
    CreateTableVersionResponse = None
    DescribeTableVersionRequest = None
    DescribeTableVersionResponse = None

from lance_namespace_impls.rest_client import (
    InvalidInputException,
    NamespaceNotFoundException,
    TableNotFoundException,
)

try:
    import sys
    import os
    goosefs_metastore_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'goosefs-metastore-client')
    if os.path.exists(goosefs_metastore_path):
        sys.path.insert(0, goosefs_metastore_path)
        # Proto-generated files in the grpc_files directory use non-package-level imports
        # (e.g., import common_pb2), so we need to add the grpc_files directory to sys.path as well
        grpc_files_path = os.path.join(goosefs_metastore_path, 'grpc_files')
        if os.path.exists(grpc_files_path) and grpc_files_path not in sys.path:
            sys.path.insert(0, grpc_files_path)
    from goosefs_metastore_client.goosefs_metastore_client import GoosefsMetastoreClient
    GOOSEFS_CLIENT_AVAILABLE = True
except ImportError:
    GoosefsMetastoreClient = None
    GOOSEFS_CLIENT_AVAILABLE = False

logger = logging.getLogger(__name__)

# Constants
LANCE_CATALOG = "lance"


def _parse_goosefs_uri(uri: str) -> tuple:
    """
    Parse GooseFS URI to extract host and port.

    Args:
        uri: GooseFS URI (e.g., "goosefs://localhost:9220")

    Returns:
        Tuple of (host, port)
    """
    from urllib.parse import urlparse

    parsed = urlparse(uri)
    if parsed.scheme not in ("goosefs"):
        raise ValueError(
            f"Invalid GooseFS URI scheme: {parsed.scheme}. "
            "Expected 'goosefs://'"
        )
    host = parsed.hostname or "localhost"
    port = parsed.port or 9220
    return host, port


class GooseFSNamespace(LanceNamespace):
    """
    Lance GooseFS Namespace implementation using GooseFS Table Master.

    This implementation uses GooseFS's Table Master service to manage table metadata,
    while storing the actual Lance table data in the configured storage root.

    Namespace hierarchy:
    - Root level (empty id): Lists all databases
    - Database level (id=["db_name"]): Lists all tables in the database

    Example:
        >>> from lance_namespace_impls import LanceNamespaces
        >>> namespace = LanceNamespaces.connect("goosefs", {
        ...     "uri": "goosefs://localhost:9220",
        ...     "root": "/data/lance",
        ... })
        >>> # List all databases
        >>> response = namespace.list_namespaces(ListNamespacesRequest())
        >>> print(response.namespaces)
        ['db1', 'db2']
    """

    def __init__(self, **properties):
        """
        Initialize the GooseFS namespace.

        Args:
            uri: GooseFS master URI (e.g., "goosefs://localhost:9220")
            root: Storage root location for Lance tables
            timeout: Timeout in seconds (default: 30)
            max_retries: Maximum number of retry attempts (default: 3)
            authentication_enabled: Whether to enable SASL authentication (default: True)
            username: Username for authentication (optional)
            impersonation_user: Optional user to impersonate
            **properties: Additional configuration properties
        """
        if not GOOSEFS_CLIENT_AVAILABLE:
            raise ImportError(
                "GooseFS metastore client not found. "
                "Please ensure goosefs-metastore-client is installed."
            )
        
        # Parse URI if provided
        if "uri" in properties:
            self.host, self.port = _parse_goosefs_uri(properties["uri"])
        else:
            self.host = properties.get("host", "localhost")
            self.port = int(properties.get("port", 9220))

        self.root = properties.get("root", os.getcwd())
        self.timeout = int(properties.get("timeout", 30))
        self.max_retries = int(properties.get("max_retries", 3))
        self.authentication_enabled = properties.get("authentication_enabled", "true").lower() == "true"
        self.username = properties.get("username")
        self.impersonation_user = properties.get("impersonation_user")

        self._properties = properties.copy()
        self._client: Optional[GoosefsMetastoreClient] = None

    def namespace_id(self) -> str:
        """Return a human-readable unique identifier for this namespace instance."""
        return f"GooseFSNamespace {{ host: {self.host!r}, port: {self.port} }}"

    @property
    def client(self) -> GoosefsMetastoreClient:
        """Get the GooseFS client, initializing it if necessary."""
        if self._client is None:
            self._client = GoosefsMetastoreClient.from_properties(
                **self._properties
            )
            self._client.connect()
        return self._client

    def _is_root_namespace(self, identifier: Optional[List[str]]) -> bool:
        """Check if the identifier refers to the root namespace."""
        return not identifier or len(identifier) == 0

    def _normalize_identifier(self, identifier: List[str]) -> tuple:
        """
        Normalize identifier to (database, table) tuple.

        GooseFS uses a 2-level namespace: database > table

        Args:
            identifier: List of namespace/table identifiers

        Returns:
            Tuple of (database_name, table_name)

        Raises:
            ValueError: If identifier has invalid number of levels
        """
        if len(identifier) == 1:
            # Just table name, use default database
            return ("default", identifier[0])
        elif len(identifier) == 2:
            return (identifier[0], identifier[1])
        else:
            raise ValueError(f"Invalid identifier: {identifier}")

    def _get_table_location(self, database: str, table: str) -> str:
        """Get the storage location for a table."""
        return os.path.join(self.root, database, table)

    def list_namespaces(
        self, request: ListNamespacesRequest
    ) -> ListNamespacesResponse:
        """
        List namespaces at the given level.

        Args:
            request: The list namespaces request

        Returns:
            List of namespace names
        """
        try:
            from grpc_files.table_master_pb2 import ListNamespacesPRequest
            grpc_request = ListNamespacesPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.page_token:
                grpc_request.page_token = request.page_token
            if request.limit is not None:
                grpc_request.limit = request.limit
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.list_namespaces(grpc_request)
            if isinstance(result, dict):
                return ListNamespacesResponse(**result)
            return ListNamespacesResponse(namespaces=[])

        except Exception as e:
            logger.error(f"Failed to list namespaces: {e}")
            raise

    def describe_namespace(
        self, request: DescribeNamespaceRequest
    ) -> DescribeNamespaceResponse:
        """
        Describe a namespace.

        Args:
            request: The describe namespace request

        Returns:
            Namespace properties
        """
        try:
            from grpc_files.table_master_pb2 import DescribeNamespacePRequest
            grpc_request = DescribeNamespacePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.describe_namespace(grpc_request)
            if isinstance(result, dict):
                return DescribeNamespaceResponse(properties=result)
            return DescribeNamespaceResponse(properties={})

        except Exception as e:
            if "not found" in str(e).lower():
                raise NamespaceNotFoundException(
                    f"Namespace {request.id} does not exist"
                )
            logger.error(f"Failed to describe namespace {request.id}: {e}")
            raise

    def create_namespace(
        self, request: CreateNamespaceRequest
    ) -> CreateNamespaceResponse:
        """
        Create a new namespace.

        Args:
            request: The create namespace request

        Returns:
            Create namespace response
        """
        try:
            from grpc_files.table_master_pb2 import CreateNamespacePRequest
            grpc_request = CreateNamespacePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if hasattr(request, 'properties') and request.properties:
                grpc_request.properties.update(request.properties)
            if request.mode:
                grpc_request.mode = request.mode
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.create_namespace(grpc_request)
            if isinstance(result, dict):
                return CreateNamespaceResponse(**result)
            return CreateNamespaceResponse()

        except Exception as e:
            logger.error(f"Failed to create namespace {request.id}: {e}")
            raise

    def drop_namespace(
        self, request: DropNamespaceRequest
    ) -> DropNamespaceResponse:
        """
        Drop a namespace.

        Args:
            request: The drop namespace request

        Returns:
            Drop namespace response
        """
        try:
            from grpc_files.table_master_pb2 import DropNamespacePRequest
            grpc_request = DropNamespacePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.mode:
                grpc_request.mode = request.mode
            if request.behavior:
                grpc_request.behavior = request.behavior
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.drop_namespace(grpc_request)
            if isinstance(result, dict):
                return DropNamespaceResponse(**result)
            return DropNamespaceResponse()

        except Exception as e:
            if "not found" in str(e).lower():
                raise NamespaceNotFoundException(
                    f"Namespace {request.id} does not exist"
                )
            logger.error(f"Failed to drop namespace {request.id}: {e}")
            raise

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        """
        List tables in a namespace.

        Args:
            request: The list tables request

        Returns:
            List of table names
        """
        try:
            from grpc_files.table_master_pb2 import ListTablesPRequest
            grpc_request = ListTablesPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.page_token:
                grpc_request.page_token = request.page_token
            if request.limit is not None:
                grpc_request.limit = request.limit
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.list_tables(grpc_request)
            if isinstance(result, dict):
                return ListTablesResponse(**result)
            return ListTablesResponse(tables=[])

        except Exception as e:
            if "not found" in str(e).lower():
                raise NamespaceNotFoundException(
                    f"Namespace {request.id} does not exist"
                )
            logger.error(f"Failed to list tables in namespace {request.id}: {e}")
            raise

    def describe_table(
        self, request: DescribeTableRequest
    ) -> DescribeTableResponse:
        """
        Describe a table.

        Args:
            request: The describe table request

        Returns:
            Table description with location
        """
        try:
            from grpc_files.table_master_pb2 import DescribeTablePRequest
            grpc_request = DescribeTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.version is not None:
                grpc_request.version = request.version
            if hasattr(request, 'load_detailed_metadata') and request.load_detailed_metadata:
                grpc_request.load_detailed_metadata = request.load_detailed_metadata
            if hasattr(request, 'with_table_uri') and request.with_table_uri:
                grpc_request.with_table_uri = request.with_table_uri
            if hasattr(request, 'vend_credentials') and request.vend_credentials:
                grpc_request.vend_credentials = request.vend_credentials
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.describe_table(grpc_request)
            if isinstance(result, dict):
                return DescribeTableResponse(**result)
            return DescribeTableResponse()

        except Exception as e:
            if "not found" in str(e).lower():
                raise TableNotFoundException(f"Table {request.id} does not exist")
            logger.error(f"Failed to describe table {request.id}: {e}")
            raise

    def declare_table(
        self, request: DeclareTableRequest
    ) -> DeclareTableResponse:
        """
        Declare a table (register table metadata).

        Args:
            request: The declare table request

        Returns:
            Declare table response with location
        """
        try:
            from grpc_files.table_master_pb2 import DeclareTablePRequest
            grpc_request = DeclareTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.location:
                grpc_request.location = request.location
            if hasattr(request, 'properties') and request.properties:
                grpc_request.properties.update(request.properties)
            if hasattr(request, 'vend_credentials') and request.vend_credentials is not None:
                grpc_request.vend_credentials = request.vend_credentials
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.declare_table(grpc_request)
            if isinstance(result, dict):
                return DeclareTableResponse(**result)
            return DeclareTableResponse(location=request.location)

        except Exception as e:
            logger.error(f"Failed to declare table {request.id}: {e}")
            raise

    def deregister_table(
        self, request: DeregisterTableRequest
    ) -> DeregisterTableResponse:
        """
        Deregister a table.

        Args:
            request: The deregister table request

        Returns:
            Deregister table response
        """
        try:
            from grpc_files.table_master_pb2 import DeregisterTablePRequest
            grpc_request = DeregisterTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.deregister_table(grpc_request)
            if isinstance(result, dict):
                return DeregisterTableResponse(**result)
            return DeregisterTableResponse()

        except Exception as e:
            if "not found" in str(e).lower():
                raise TableNotFoundException(f"Table {request.id} does not exist")
            logger.error(f"Failed to deregister table {request.id}: {e}")
            raise

    def namespace_exists(self, request: NamespaceExistsRequest) -> None:
        """
        Check if a namespace exists.

        Raises NamespaceNotFoundException if the namespace does not exist.

        Args:
            request: The namespace exists request
        """
        try:
            from grpc_files.table_master_pb2 import NamespaceExistsPRequest
            grpc_request = NamespaceExistsPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.catalog = LANCE_CATALOG
            exists = self.client.namespace_exists(grpc_request)
            if not exists:
                raise NamespaceNotFoundException(
                    f"Namespace {request.id} does not exist"
                )
        except NamespaceNotFoundException:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NamespaceNotFoundException(
                    f"Namespace {request.id} does not exist"
                )
            logger.error(f"Failed to check namespace existence {request.id}: {e}")
            raise

    def table_exists(self, request: TableExistsRequest) -> None:
        """
        Check if a table exists.

        Raises NamespaceNotFoundException if the namespace does not exist.
        Raises TableNotFoundException if the table does not exist.

        Args:
            request: The table exists request
        """
        try:
            from grpc_files.table_master_pb2 import TableExistsPRequest
            grpc_request = TableExistsPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.version is not None:
                grpc_request.version = request.version
            grpc_request.catalog = LANCE_CATALOG
            exists = self.client.table_exists(grpc_request)
            if not exists:
                raise TableNotFoundException(
                    f"Table {request.id} does not exist"
                )
        except (NamespaceNotFoundException, TableNotFoundException):
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                err_msg = str(e).lower()
                if "database" in err_msg or "namespace" in err_msg:
                    raise NamespaceNotFoundException(
                        f"Namespace for {request.id} does not exist"
                    )
                raise TableNotFoundException(
                    f"Table {request.id} does not exist"
                )
            logger.error(f"Failed to check table existence {request.id}: {e}")
            raise

    def register_table(
        self, request: RegisterTableRequest
    ) -> RegisterTableResponse:
        """
        Register a table.

        Args:
            request: The register table request

        Returns:
            Register table response with location and storage_options
        """
        try:
            from grpc_files.table_master_pb2 import RegisterTablePRequest
            grpc_request = RegisterTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.location = request.location
            if request.mode:
                grpc_request.mode = request.mode
            if hasattr(request, 'properties') and request.properties:
                grpc_request.properties.update(request.properties)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.register_table(grpc_request)
            if isinstance(result, dict):
                return RegisterTableResponse(**result)
            return RegisterTableResponse(
                location=request.location,
                properties=request.properties,
            )
        except Exception as e:
            logger.error(f"Failed to register table {request.id}: {e}")
            raise

    def drop_table(self, request: DropTableRequest) -> DropTableResponse:
        """
        Drop a table.

        Args:
            request: The drop table request

        Returns:
            Drop table response
        """
        try:
            from grpc_files.table_master_pb2 import DropTablePRequest
            grpc_request = DropTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.catalog = LANCE_CATALOG
            self.client.drop_table(grpc_request)
            return DropTableResponse(id=request.id)
        except Exception as e:
            logger.error(f"Failed to drop table {request.id}: {e}")
            raise

    def count_table_rows(self, request: CountTableRowsRequest) -> int:
        """
        Count rows in a table.

        Args:
            request: The count table rows request

        Returns:
            Number of rows
        """
        try:
            from grpc_files.table_master_pb2 import CountTableRowsPRequest
            grpc_request = CountTableRowsPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.predicate:
                grpc_request.predicate = request.predicate
            if request.version is not None:
                grpc_request.version = request.version
            grpc_request.catalog = LANCE_CATALOG
            return self.client.count_table_rows(grpc_request)
        except Exception as e:
            logger.error(f"Failed to count table rows for {request.id}: {e}")
            raise

    def create_table(
        self, request: CreateTableRequest, request_data: bytes
    ) -> CreateTableResponse:
        """
        Create a new table with data from Arrow IPC stream.

        Args:
            request: The create table request
            request_data: Arrow IPC stream data

        Returns:
            Create table response
        """
        try:
            from grpc_files.table_master_pb2 import CreateTablePRequest
            grpc_request = CreateTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.mode:
                grpc_request.mode = request.mode
            if hasattr(request, 'properties') and request.properties:
                grpc_request.properties.update(request.properties)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.create_table(grpc_request, request_data)
            if isinstance(result, dict):
                return CreateTableResponse(**result)
            return CreateTableResponse()
        except Exception as e:
            logger.error(f"Failed to create table {request.id}: {e}")
            raise

    def create_empty_table(
        self, request: CreateEmptyTableRequest
    ) -> CreateEmptyTableResponse:
        """
        Create an empty table.

        Args:
            request: The create empty table request

        Returns:
            Create empty table response
        """
        try:
            from grpc_files.table_master_pb2 import CreateEmptyTablePRequest
            grpc_request = CreateEmptyTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.location:
                grpc_request.location = request.location
            if hasattr(request, 'properties') and request.properties:
                grpc_request.properties.update(request.properties)
            if hasattr(request, 'vend_credentials') and request.vend_credentials is not None:
                grpc_request.vend_credentials = request.vend_credentials
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.create_empty_table(grpc_request)
            if isinstance(result, dict):
                return CreateEmptyTableResponse(**result)
            return CreateEmptyTableResponse(location=request.location)
        except Exception as e:
            logger.error(f"Failed to create empty table {request.id}: {e}")
            raise

    def insert_into_table(
        self, request: InsertIntoTableRequest, request_data: bytes
    ) -> InsertIntoTableResponse:
        """
        Insert data into a table.

        Args:
            request: The insert into table request
            request_data: Arrow IPC stream data

        Returns:
            Insert into table response
        """
        try:
            from grpc_files.table_master_pb2 import InsertIntoTablePRequest
            grpc_request = InsertIntoTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.mode:
                grpc_request.mode = request.mode
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.insert_into_table(grpc_request, request_data)
            if isinstance(result, dict):
                return InsertIntoTableResponse(**result)
            return InsertIntoTableResponse()
        except Exception as e:
            logger.error(f"Failed to insert into table {request.id}: {e}")
            raise

    def merge_insert_into_table(
        self, request: MergeInsertIntoTableRequest, request_data: bytes
    ) -> MergeInsertIntoTableResponse:
        """
        Merge insert data into a table.

        Args:
            request: The merge insert into table request
            request_data: Arrow IPC stream data

        Returns:
            Merge insert into table response
        """
        try:
            from grpc_files.table_master_pb2 import MergeInsertIntoTablePRequest
            grpc_request = MergeInsertIntoTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.on:
                grpc_request.on = request.on
            if request.when_matched_update_all:
                grpc_request.when_matched_update_all = request.when_matched_update_all
            if hasattr(request, 'when_matched_update_all_filt') and request.when_matched_update_all_filt:
                grpc_request.when_matched_update_all_filt = request.when_matched_update_all_filt
            if request.when_not_matched_insert_all:
                grpc_request.when_not_matched_insert_all = request.when_not_matched_insert_all
            if request.when_not_matched_by_source_delete:
                grpc_request.when_not_matched_by_source_delete = request.when_not_matched_by_source_delete
            if hasattr(request, 'when_not_matched_by_source_delete_filt') and request.when_not_matched_by_source_delete_filt:
                grpc_request.when_not_matched_by_source_delete_filt = request.when_not_matched_by_source_delete_filt
            if hasattr(request, 'timeout') and request.timeout:
                grpc_request.timeout = request.timeout
            if hasattr(request, 'use_index') and request.use_index is not None:
                grpc_request.use_index = request.use_index
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.merge_insert_into_table(grpc_request, request_data)
            if isinstance(result, dict):
                return MergeInsertIntoTableResponse(**result)
            return MergeInsertIntoTableResponse()
        except Exception as e:
            logger.error(f"Failed to merge insert into table {request.id}: {e}")
            raise

    def update_table(self, request: UpdateTableRequest) -> UpdateTableResponse:
        """
        Update table records.

        Args:
            request: The update table request

        Returns:
            Update table response
        """
        try:
            from grpc_files.table_master_pb2 import UpdateTablePRequest
            grpc_request = UpdateTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.updates:
                import json
                for update in request.updates:
                    if isinstance(update, (list, tuple)):
                        # Each update pair [column, expression] → JSON string
                        grpc_request.updates.append(json.dumps(update))
                    elif isinstance(update, str):
                        grpc_request.updates.append(update)
                    else:
                        grpc_request.updates.append(str(update))
            if request.predicate:
                grpc_request.predicate = request.predicate
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.update_table(grpc_request)
            if isinstance(result, dict):
                return UpdateTableResponse(**result)
            return UpdateTableResponse(updated_rows=0, version=0)
        except Exception as e:
            logger.error(f"Failed to update table {request.id}: {e}")
            raise

    def delete_from_table(
        self, request: DeleteFromTableRequest
    ) -> DeleteFromTableResponse:
        """
        Delete records from a table.

        Args:
            request: The delete from table request

        Returns:
            Delete from table response
        """
        try:
            from grpc_files.table_master_pb2 import DeleteFromTablePRequest
            grpc_request = DeleteFromTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.predicate = request.predicate
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.delete_from_table(grpc_request)
            if isinstance(result, dict):
                return DeleteFromTableResponse(**result)
            return DeleteFromTableResponse()
        except Exception as e:
            logger.error(f"Failed to delete from table {request.id}: {e}")
            raise

    def query_table(self, request: QueryTableRequest) -> bytes:
        """
        Query a table.

        Args:
            request: The query table request

        Returns:
            Query result as bytes (Arrow IPC stream)
        """
        try:
            from grpc_files.table_master_pb2 import QueryTablePRequest
            grpc_request = QueryTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if hasattr(request, 'bypass_vector_index') and request.bypass_vector_index is not None:
                grpc_request.bypass_vector_index = request.bypass_vector_index
            if request.columns:
                import json
                if isinstance(request.columns, list):
                    grpc_request.columns = json.dumps(request.columns)
                elif hasattr(request.columns, 'column_names') and request.columns.column_names:
                    grpc_request.columns = json.dumps(request.columns.column_names)
                elif hasattr(request.columns, 'model_dump'):
                    grpc_request.columns = json.dumps(request.columns.model_dump(exclude_none=True))
                else:
                    grpc_request.columns = str(request.columns)
            if hasattr(request, 'distance_type') and request.distance_type:
                grpc_request.distance_type = request.distance_type
            if hasattr(request, 'ef') and request.ef is not None:
                grpc_request.ef = request.ef
            if hasattr(request, 'fast_search') and request.fast_search is not None:
                grpc_request.fast_search = request.fast_search
            if request.filter:
                grpc_request.filter = request.filter
            if hasattr(request, 'full_text_query') and request.full_text_query:
                import json
                grpc_request.full_text_query = json.dumps(request.full_text_query) if isinstance(request.full_text_query, dict) else str(request.full_text_query)
            if request.k is not None:
                grpc_request.k = request.k
            if hasattr(request, 'lower_bound') and request.lower_bound is not None:
                grpc_request.lower_bound = request.lower_bound
            if hasattr(request, 'nprobes') and request.nprobes is not None:
                grpc_request.nprobes = request.nprobes
            if hasattr(request, 'offset') and request.offset is not None:
                grpc_request.offset = request.offset
            if hasattr(request, 'prefilter') and request.prefilter is not None:
                grpc_request.prefilter = request.prefilter
            if hasattr(request, 'refine_factor') and request.refine_factor is not None:
                grpc_request.refine_factor = request.refine_factor
            if hasattr(request, 'upper_bound') and request.upper_bound is not None:
                grpc_request.upper_bound = request.upper_bound
            if request.vector:
                # request.vector may be a QueryTableRequestVector pydantic model
                if hasattr(request.vector, 'single_vector') and request.vector.single_vector:
                    grpc_request.vector.extend(request.vector.single_vector)
                elif hasattr(request.vector, 'multi_vector') and request.vector.multi_vector:
                    # Flatten multi_vector for gRPC repeated float field
                    for vec in request.vector.multi_vector:
                        grpc_request.vector.extend(vec)
                elif isinstance(request.vector, (list, tuple)):
                    grpc_request.vector.extend(request.vector)
                else:
                    logger.warning(f"Unsupported vector type: {type(request.vector)}")
            if hasattr(request, 'vector_column') and request.vector_column:
                grpc_request.vector_column = request.vector_column
            if hasattr(request, 'version') and request.version is not None:
                grpc_request.version = request.version
            if hasattr(request, 'with_row_id') and request.with_row_id is not None:
                grpc_request.with_row_id = request.with_row_id
            grpc_request.catalog = LANCE_CATALOG
            return self.client.query_table(grpc_request)
        except Exception as e:
            logger.error(f"Failed to query table {request.id}: {e}")
            raise

    def create_table_index(
        self, request: CreateTableIndexRequest
    ) -> CreateTableIndexResponse:
        """
        Create a table index.

        Args:
            request: The create table index request

        Returns:
            Create table index response
        """
        try:
            from grpc_files.table_master_pb2 import CreateTableIndexPRequest
            grpc_request = CreateTableIndexPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.column:
                grpc_request.column = request.column
            if request.index_type:
                grpc_request.index_type = request.index_type
            if request.name:
                grpc_request.name = request.name
            if request.distance_type:
                grpc_request.distance_type = request.distance_type
            if hasattr(request, 'with_position') and request.with_position is not None:
                grpc_request.with_position = request.with_position
            if hasattr(request, 'base_tokenizer') and request.base_tokenizer:
                grpc_request.base_tokenizer = request.base_tokenizer
            if hasattr(request, 'language') and request.language:
                grpc_request.language = request.language
            if hasattr(request, 'max_token_length') and request.max_token_length is not None:
                grpc_request.max_token_length = request.max_token_length
            if hasattr(request, 'lower_case') and request.lower_case is not None:
                grpc_request.lower_case = request.lower_case
            if hasattr(request, 'stem') and request.stem is not None:
                grpc_request.stem = request.stem
            if hasattr(request, 'remove_stop_words') and request.remove_stop_words is not None:
                grpc_request.remove_stop_words = request.remove_stop_words
            if hasattr(request, 'ascii_folding') and request.ascii_folding is not None:
                grpc_request.ascii_folding = request.ascii_folding
            grpc_request.catalog = LANCE_CATALOG
            self.client.create_table_index(grpc_request)
            return CreateTableIndexResponse()
        except Exception as e:
            logger.error(f"Failed to create table index on {request.id}: {e}")
            raise

    def create_table_scalar_index(
        self, request: CreateTableIndexRequest
    ) -> CreateTableScalarIndexResponse:
        """
        Create a scalar index on a table.

        Args:
            request: The create table index request

        Returns:
            Create table scalar index response
        """
        try:
            from grpc_files.table_master_pb2 import CreateTableScalarIndexPRequest
            grpc_request = CreateTableScalarIndexPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.column:
                grpc_request.columns.append(request.column)
            if request.index_type:
                grpc_request.index_type = request.index_type
            grpc_request.catalog = LANCE_CATALOG
            self.client.create_table_scalar_index(grpc_request)
            return CreateTableScalarIndexResponse()
        except Exception as e:
            logger.error(f"Failed to create scalar index on {request.id}: {e}")
            raise

    def list_table_indices(
        self, request: ListTableIndicesRequest
    ) -> ListTableIndicesResponse:
        """
        List table indices.

        Args:
            request: The list table indices request

        Returns:
            List table indices response
        """
        try:
            from grpc_files.table_master_pb2 import ListTableIndicesPRequest
            grpc_request = ListTableIndicesPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.page_token:
                grpc_request.page_token = request.page_token
            if request.limit is not None:
                grpc_request.limit = request.limit
            if request.version is not None:
                grpc_request.version = request.version
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.list_table_indices(grpc_request)
            if isinstance(result, dict):
                return ListTableIndicesResponse(**result)
            return ListTableIndicesResponse(indexes=[])
        except Exception as e:
            logger.error(f"Failed to list table indices for {request.id}: {e}")
            raise

    def describe_table_index_stats(
        self, request: DescribeTableIndexStatsRequest
    ) -> DescribeTableIndexStatsResponse:
        """
        Describe table index statistics.

        Args:
            request: The describe table index stats request

        Returns:
            Describe table index stats response
        """
        try:
            from grpc_files.table_master_pb2 import DescribeTableIndexStatsPRequest
            grpc_request = DescribeTableIndexStatsPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.index_name:
                grpc_request.index_name = request.index_name
            if request.version is not None:
                grpc_request.version = request.version
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.describe_table_index_stats(grpc_request)
            if isinstance(result, dict):
                return DescribeTableIndexStatsResponse(**result)
            return DescribeTableIndexStatsResponse()
        except Exception as e:
            logger.error(f"Failed to describe index stats for {request.id}: {e}")
            raise

    def drop_table_index(
        self, request: DropTableIndexRequest
    ) -> DropTableIndexResponse:
        """
        Drop a table index.

        Args:
            request: The drop table index request

        Returns:
            Drop table index response
        """
        try:
            from grpc_files.table_master_pb2 import DropTableIndexPRequest
            grpc_request = DropTableIndexPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.index_name:
                grpc_request.index_name = request.index_name
            grpc_request.catalog = LANCE_CATALOG
            self.client.drop_table_index(grpc_request)
            return DropTableIndexResponse()
        except Exception as e:
            logger.error(f"Failed to drop index from table {request.id}: {e}")
            raise

    def list_all_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        """
        List all tables across all namespaces.

        Args:
            request: The list tables request

        Returns:
            List tables response
        """
        try:
            from grpc_files.table_master_pb2 import ListAllTablesPRequest
            grpc_request = ListAllTablesPRequest()
            if request.page_token:
                grpc_request.page_token = request.page_token
            if request.limit is not None:
                grpc_request.limit = request.limit
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.list_all_tables(grpc_request)
            if isinstance(result, dict):
                return ListTablesResponse(**result)
            return ListTablesResponse(tables=[])
        except Exception as e:
            logger.error(f"Failed to list all tables: {e}")
            raise

    def restore_table(
        self, request: RestoreTableRequest
    ) -> RestoreTableResponse:
        """
        Restore a table to a specific version.

        Args:
            request: The restore table request

        Returns:
            Restore table response
        """
        try:
            from grpc_files.table_master_pb2 import RestoreTablePRequest
            grpc_request = RestoreTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.version = request.version
            grpc_request.catalog = LANCE_CATALOG
            self.client.restore_table(grpc_request)
            return RestoreTableResponse()
        except Exception as e:
            logger.error(f"Failed to restore table {request.id}: {e}")
            raise

    def rename_table(
        self, request: RenameTableRequest
    ) -> RenameTableResponse:
        """
        Rename a table.

        Args:
            request: The rename table request

        Returns:
            Rename table response
        """
        try:
            from grpc_files.table_master_pb2 import RenameTablePRequest
            grpc_request = RenameTablePRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.new_table_name = request.new_table_name
            if request.new_namespace_id:
                grpc_request.new_namespace_id.extend(request.new_namespace_id)
            grpc_request.catalog = LANCE_CATALOG
            self.client.rename_table(grpc_request)
            return RenameTableResponse()
        except Exception as e:
            logger.error(f"Failed to rename table {request.id}: {e}")
            raise

    def list_table_versions(
        self, request: ListTableVersionsRequest
    ) -> ListTableVersionsResponse:
        """
        List all versions of a table.

        Args:
            request: The list table versions request

        Returns:
            List table versions response
        """
        try:
            from grpc_files.table_master_pb2 import ListTableVersionsPRequest
            grpc_request = ListTableVersionsPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.page_token:
                grpc_request.page_token = request.page_token
            if request.limit is not None:
                grpc_request.limit = request.limit
            if hasattr(request, 'descending') and request.descending is not None:
                grpc_request.descending = request.descending
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.list_table_versions(grpc_request)
            if isinstance(result, dict):
                return ListTableVersionsResponse(**result)
            return ListTableVersionsResponse(versions=[])
        except Exception as e:
            logger.error(f"Failed to list table versions for {request.id}: {e}")
            raise

    def create_table_version(
        self, request: CreateTableVersionRequest
    ) -> CreateTableVersionResponse:
        """
        Create a new table version entry.

        This operation supports put_if_not_exists semantics,
        where the operation fails if the version already exists.

        Args:
            request: The create table version request

        Returns:
            Create table version response
        """
        try:
            from grpc_files.table_master_pb2 import CreateTableVersionPRequest
            grpc_request = CreateTableVersionPRequest()
            grpc_request.id.extend(request.id)
            if request.version is not None:
                grpc_request.version = request.version
            if request.manifest_path is not None:
                grpc_request.manifest_path = request.manifest_path
            if hasattr(request, 'manifest_size') and request.manifest_size is not None:
                grpc_request.manifest_size = request.manifest_size
            if hasattr(request, 'e_tag') and request.e_tag is not None:
                grpc_request.e_tag = request.e_tag
            if hasattr(request, 'metadata') and request.metadata:
                grpc_request.metadata.update(request.metadata)
            if hasattr(request, 'naming_scheme') and request.naming_scheme is not None:
                grpc_request.naming_scheme = request.naming_scheme
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.create_table_version(grpc_request)
            return CreateTableVersionResponse(**result) if isinstance(result, dict) else result
        except Exception as e:
            logger.error(f"Failed to create table version for {request.id}: {e}")
            raise

    def describe_table_version(
        self, request: DescribeTableVersionRequest
    ) -> DescribeTableVersionResponse:
        """
        Describe a specific table version.

        Returns the manifest path and metadata for the specified version.

        Args:
            request: The describe table version request

        Returns:
            Describe table version response
        """
        try:
            from grpc_files.table_master_pb2 import DescribeTableVersionPRequest
            grpc_request = DescribeTableVersionPRequest()
            grpc_request.id.extend(request.id)
            if request.version is not None:
                grpc_request.version = request.version
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.describe_table_version(grpc_request)
            return DescribeTableVersionResponse(**result) if isinstance(result, dict) else result
        except Exception as e:
            logger.error(f"Failed to describe table version for {request.id}: {e}")
            raise

    def batch_delete_table_versions(
        self, request: BatchDeleteTableVersionsRequest
    ) -> BatchDeleteTableVersionsResponse:
        """
        Delete table version metadata records.

        This operation deletes version tracking records, NOT the actual table data.

        Args:
            request: The batch delete table versions request

        Returns:
            Batch delete table versions response
        """
        try:
            from grpc_files.table_master_pb2 import BatchDeleteTableVersionsPRequest, VersionRange
            grpc_request = BatchDeleteTableVersionsPRequest()
            grpc_request.id.extend(request.id)
            if request.versions is not None:
                for v in request.versions:
                    if isinstance(v, dict):
                        vr = VersionRange()
                        if 'start_version' in v:
                            vr.start_version = v['start_version']
                        if 'end_version' in v:
                            vr.end_version = v['end_version']
                        grpc_request.ranges.append(vr)
                    else:
                        # Treat as single version: create a range [v, v+1)
                        vr = VersionRange()
                        vr.start_version = v
                        vr.end_version = v + 1
                        grpc_request.ranges.append(vr)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.batch_delete_table_versions(grpc_request)
            return BatchDeleteTableVersionsResponse(**result) if isinstance(result, dict) else result
        except Exception as e:
            logger.error(f"Failed to batch delete table versions for {request.id}: {e}")
            raise

    def update_table_schema_metadata(
        self, request: UpdateTableSchemaMetadataRequest
    ) -> UpdateTableSchemaMetadataResponse:
        """
        Update table schema metadata.

        Args:
            request: The update table schema metadata request

        Returns:
            Update table schema metadata response
        """
        try:
            from grpc_files.table_master_pb2 import UpdateTableSchemaMetadataPRequest
            grpc_request = UpdateTableSchemaMetadataPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.metadata:
                grpc_request.metadata.update(request.metadata)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.update_table_schema_metadata(grpc_request)
            if isinstance(result, dict):
                return UpdateTableSchemaMetadataResponse(**result)
            return UpdateTableSchemaMetadataResponse()
        except Exception as e:
            logger.error(f"Failed to update table schema metadata for {request.id}: {e}")
            raise

    def get_table_stats(
        self, request: GetTableStatsRequest
    ) -> GetTableStatsResponse:
        """
        Get table statistics.

        Args:
            request: The get table stats request

        Returns:
            Get table stats response
        """
        try:
            from grpc_files.table_master_pb2 import GetTableStatsPRequest
            grpc_request = GetTableStatsPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.get_table_stats(grpc_request)
            if isinstance(result, dict):
                return GetTableStatsResponse(**result)
            return result
        except Exception as e:
            logger.error(f"Failed to get table stats for {request.id}: {e}")
            raise

    def explain_table_query_plan(
        self, request: ExplainTableQueryPlanRequest
    ) -> str:
        """
        Explain a table query plan.

        Args:
            request: The explain table query plan request

        Returns:
            Query plan explanation string
        """
        try:
            from grpc_files.table_master_pb2 import ExplainTableQueryPlanPRequest
            grpc_request = ExplainTableQueryPlanPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.query:
                grpc_request.query = request.query
            if request.verbose is not None:
                grpc_request.verbose = request.verbose
            grpc_request.catalog = LANCE_CATALOG
            return self.client.explain_table_query_plan(grpc_request)
        except Exception as e:
            logger.error(f"Failed to explain query plan for {request.id}: {e}")
            raise

    def analyze_table_query_plan(
        self, request: AnalyzeTableQueryPlanRequest
    ) -> str:
        """
        Analyze a table query plan.

        Args:
            request: The analyze table query plan request

        Returns:
            Query plan analysis string
        """
        try:
            from grpc_files.table_master_pb2 import AnalyzeTableQueryPlanPRequest
            grpc_request = AnalyzeTableQueryPlanPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.filter:
                grpc_request.query = request.filter
            grpc_request.catalog = LANCE_CATALOG
            return self.client.analyze_table_query_plan(grpc_request)
        except Exception as e:
            logger.error(f"Failed to analyze query plan for {request.id}: {e}")
            raise

    def alter_table_add_columns(
        self, request: AlterTableAddColumnsRequest
    ) -> AlterTableAddColumnsResponse:
        """
        Add columns to a table.

        Args:
            request: The alter table add columns request

        Returns:
            Alter table add columns response
        """
        try:
            from grpc_files.table_master_pb2 import AlterTableAddColumnsPRequest, NewColumnTransform
            grpc_request = AlterTableAddColumnsPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.new_columns:
                for col in request.new_columns:
                    new_col = NewColumnTransform()
                    if col.name:
                        new_col.name = col.name
                    if col.expression:
                        new_col.expression = col.expression
                    grpc_request.columns.append(new_col)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.alter_table_add_columns(grpc_request)
            if isinstance(result, dict):
                return AlterTableAddColumnsResponse(**result)
            return AlterTableAddColumnsResponse(version=0)
        except Exception as e:
            logger.error(f"Failed to add columns to table {request.id}: {e}")
            raise

    def alter_table_alter_columns(
        self, request: AlterTableAlterColumnsRequest
    ) -> AlterTableAlterColumnsResponse:
        """
        Alter columns in a table.

        Args:
            request: The alter table alter columns request

        Returns:
            Alter table alter columns response
        """
        try:
            from grpc_files.table_master_pb2 import AlterTableAlterColumnsPRequest, AlterColumnsEntry
            grpc_request = AlterTableAlterColumnsPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.alterations:
                for alt in request.alterations:
                    entry = AlterColumnsEntry()
                    if alt.path:
                        entry.path = alt.path
                    if alt.data_type:
                        import json
                        entry.data_type = json.dumps(alt.data_type) if isinstance(alt.data_type, dict) else str(alt.data_type)
                    if alt.rename:
                        entry.rename = alt.rename
                    if alt.nullable is not None:
                        entry.nullable = alt.nullable
                    grpc_request.columns.append(entry)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.alter_table_alter_columns(grpc_request)
            if isinstance(result, dict):
                return AlterTableAlterColumnsResponse(**result)
            return AlterTableAlterColumnsResponse(version=0)
        except Exception as e:
            logger.error(f"Failed to alter columns in table {request.id}: {e}")
            raise

    def alter_table_drop_columns(
        self, request: AlterTableDropColumnsRequest
    ) -> AlterTableDropColumnsResponse:
        """
        Drop columns from a table.

        Args:
            request: The alter table drop columns request

        Returns:
            Alter table drop columns response
        """
        try:
            from grpc_files.table_master_pb2 import AlterTableDropColumnsPRequest
            grpc_request = AlterTableDropColumnsPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.columns:
                grpc_request.columns.extend(request.columns)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.alter_table_drop_columns(grpc_request)
            if isinstance(result, dict):
                return AlterTableDropColumnsResponse(**result)
            return AlterTableDropColumnsResponse(version=0)
        except Exception as e:
            logger.error(f"Failed to drop columns from table {request.id}: {e}")
            raise

    def list_table_tags(
        self, request: ListTableTagsRequest
    ) -> ListTableTagsResponse:
        """
        List all tags for a table.

        Args:
            request: The list table tags request

        Returns:
            List table tags response
        """
        try:
            from grpc_files.table_master_pb2 import ListTableTagsPRequest
            grpc_request = ListTableTagsPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.page_token:
                grpc_request.page_token = request.page_token
            if request.limit is not None:
                grpc_request.limit = request.limit
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.list_table_tags(grpc_request)
            if isinstance(result, dict):
                return ListTableTagsResponse(**result)
            return ListTableTagsResponse(tags={})
        except Exception as e:
            logger.error(f"Failed to list tags for table {request.id}: {e}")
            raise

    def get_table_tag_version(
        self, request: GetTableTagVersionRequest
    ) -> GetTableTagVersionResponse:
        """
        Get the version for a specific tag.

        Args:
            request: The get table tag version request

        Returns:
            Get table tag version response
        """
        try:
            from grpc_files.table_master_pb2 import GetTableTagVersionPRequest
            grpc_request = GetTableTagVersionPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.tag = request.tag
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.get_table_tag_version(grpc_request)
            if isinstance(result, int):
                return GetTableTagVersionResponse(version=result)
            if isinstance(result, dict):
                return GetTableTagVersionResponse(**result)
            return result
        except Exception as e:
            logger.error(f"Failed to get tag version for table {request.id}: {e}")
            raise

    def create_table_tag(
        self, request: CreateTableTagRequest
    ) -> CreateTableTagResponse:
        """
        Create a tag for a table.

        Args:
            request: The create table tag request

        Returns:
            Create table tag response
        """
        try:
            from grpc_files.table_master_pb2 import CreateTableTagPRequest
            grpc_request = CreateTableTagPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.tag = request.tag
            grpc_request.version = request.version
            grpc_request.catalog = LANCE_CATALOG
            self.client.create_table_tag(grpc_request)
            return CreateTableTagResponse()
        except Exception as e:
            logger.error(f"Failed to create tag for table {request.id}: {e}")
            raise

    def delete_table_tag(
        self, request: DeleteTableTagRequest
    ) -> DeleteTableTagResponse:
        """
        Delete a tag from a table.

        Args:
            request: The delete table tag request

        Returns:
            Delete table tag response
        """
        try:
            from grpc_files.table_master_pb2 import DeleteTableTagPRequest
            grpc_request = DeleteTableTagPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.tag = request.tag
            grpc_request.catalog = LANCE_CATALOG
            self.client.delete_table_tag(grpc_request)
            return DeleteTableTagResponse()
        except Exception as e:
            logger.error(f"Failed to delete tag from table {request.id}: {e}")
            raise

    def update_table_tag(
        self, request: UpdateTableTagRequest
    ) -> UpdateTableTagResponse:
        """
        Update a tag for a table.

        Args:
            request: The update table tag request

        Returns:
            Update table tag response
        """
        try:
            from grpc_files.table_master_pb2 import UpdateTableTagPRequest
            grpc_request = UpdateTableTagPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.tag = request.tag
            grpc_request.version = request.version
            grpc_request.catalog = LANCE_CATALOG
            self.client.update_table_tag(grpc_request)
            return UpdateTableTagResponse()
        except Exception as e:
            logger.error(f"Failed to update tag for table {request.id}: {e}")
            raise

    def describe_transaction(
        self, request: DescribeTransactionRequest
    ) -> DescribeTransactionResponse:
        """
        Describe a transaction.

        Args:
            request: The describe transaction request

        Returns:
            Describe transaction response
        """
        try:
            from grpc_files.table_master_pb2 import DescribeTransactionPRequest
            grpc_request = DescribeTransactionPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.describe_transaction(grpc_request)
            if isinstance(result, dict):
                return DescribeTransactionResponse(**result)
            return result
        except Exception as e:
            logger.error(f"Failed to describe transaction {request.id}: {e}")
            raise

    def alter_transaction(
        self, request: AlterTransactionRequest
    ) -> AlterTransactionResponse:
        """
        Alter a transaction.

        Args:
            request: The alter transaction request

        Returns:
            Alter transaction response
        """
        try:
            from grpc_files.table_master_pb2 import AlterTransactionPRequest, AlterTransactionAction, AlterTransactionSetStatus, AlterTransactionSetProperty, AlterTransactionUnsetProperty
            grpc_request = AlterTransactionPRequest()
            if request.id:
                grpc_request.id.extend(request.id)
            if request.actions:
                for action in request.actions:
                    grpc_action = AlterTransactionAction()
                    if action.set_status_action:
                        set_status = AlterTransactionSetStatus()
                        if action.set_status_action.status:
                            set_status.status = action.set_status_action.status
                        grpc_action.set_status_action.CopyFrom(set_status)
                    if action.set_property_action:
                        set_prop = AlterTransactionSetProperty()
                        if action.set_property_action.key:
                            set_prop.key = action.set_property_action.key
                        if action.set_property_action.value:
                            set_prop.value = action.set_property_action.value
                        if action.set_property_action.mode:
                            set_prop.mode = action.set_property_action.mode
                        grpc_action.set_property_action.CopyFrom(set_prop)
                    if action.unset_property_action:
                        unset_prop = AlterTransactionUnsetProperty()
                        if action.unset_property_action.key:
                            unset_prop.key = action.unset_property_action.key
                        if action.unset_property_action.mode:
                            unset_prop.mode = action.unset_property_action.mode
                        grpc_action.unset_property_action.CopyFrom(unset_prop)
                    grpc_request.actions.append(grpc_action)
            grpc_request.catalog = LANCE_CATALOG
            result = self.client.alter_transaction(grpc_request)
            if isinstance(result, dict):
                return AlterTransactionResponse(**result)
            return result
        except Exception as e:
            logger.error(f"Failed to alter transaction {request.id}: {e}")
            raise

    def batch_create_table_versions(
        self, request: BatchCreateTableVersionsRequest
    ) -> BatchCreateTableVersionsResponse:
        """
        Atomically create new version entries for multiple tables.

        The operation is atomic: either all versions are created, or none are.
        Supports put_if_not_exists semantics per entry.

        Args:
            request: The batch create table versions request containing entries

        Returns:
            Batch create table versions response with transaction_id and versions
        """
        try:
            from grpc_files.table_master_pb2 import (
                BatchCreateTableVersionsPRequest,
                CreateTableVersionEntry as GrpcCreateTableVersionEntry,
            )
            grpc_request = BatchCreateTableVersionsPRequest()
            for entry in request.entries:
                grpc_entry = grpc_request.entries.add()
                grpc_entry.id.extend(entry.id)
                if entry.version is not None:
                    grpc_entry.version = entry.version
                if entry.manifest_path is not None:
                    grpc_entry.manifest_path = entry.manifest_path
                if hasattr(entry, 'manifest_size') and entry.manifest_size is not None:
                    grpc_entry.manifest_size = entry.manifest_size
                if hasattr(entry, 'e_tag') and entry.e_tag is not None:
                    grpc_entry.e_tag = entry.e_tag
                if hasattr(entry, 'metadata') and entry.metadata:
                    grpc_entry.metadata.update(entry.metadata)
                if hasattr(entry, 'naming_scheme') and entry.naming_scheme is not None:
                    grpc_entry.naming_scheme = entry.naming_scheme
            grpc_request.catalog = "lance"
            result = self.client.batch_create_table_versions(grpc_request)
            return BatchCreateTableVersionsResponse(**result) if isinstance(result, dict) else result
        except Exception as e:
            logger.error(f"Failed to batch create table versions: {e}")
            raise

    def batch_commit_tables(
        self, request: BatchCommitTablesRequest
    ) -> BatchCommitTablesResponse:
        """
        Atomically commit a batch of table operations.

        Replaces BatchCreateTableVersionsRequest with a more general interface
        that supports mixing DeclareTable, CreateTableVersion,
        DeleteTableVersions, and DeregisterTable operations in a single
        atomic transaction.

        Args:
            request: The batch commit tables request containing operations

        Returns:
            Batch commit tables response with transaction_id and results
        """
        try:
            from grpc_files.table_master_pb2 import (
                BatchCommitTablesPRequest,
                CommitTableOperation as GrpcCommitTableOperation,
                DeclareTablePRequest,
                CreateTableVersionPRequest,
                BatchDeleteTableVersionsPRequest,
                DeregisterTablePRequest,
                VersionRange,
            )
            grpc_request = BatchCommitTablesPRequest()
            for op in request.operations:
                grpc_op = grpc_request.operations.add()
                if op.declare_table is not None:
                    dt_req = DeclareTablePRequest()
                    dt_req.id.extend(op.declare_table.id)
                    if op.declare_table.location is not None:
                        dt_req.location = op.declare_table.location
                    if hasattr(op.declare_table, 'properties') and op.declare_table.properties:
                        dt_req.properties.update(op.declare_table.properties)
                    grpc_op.declare_table.CopyFrom(dt_req)
                if op.create_table_version is not None:
                    ctv_req = CreateTableVersionPRequest()
                    ctv_req.id.extend(op.create_table_version.id)
                    if op.create_table_version.version is not None:
                        ctv_req.version = op.create_table_version.version
                    if op.create_table_version.manifest_path is not None:
                        ctv_req.manifest_path = op.create_table_version.manifest_path
                    if hasattr(op.create_table_version, 'manifest_size') and op.create_table_version.manifest_size is not None:
                        ctv_req.manifest_size = op.create_table_version.manifest_size
                    if hasattr(op.create_table_version, 'e_tag') and op.create_table_version.e_tag is not None:
                        ctv_req.e_tag = op.create_table_version.e_tag
                    if hasattr(op.create_table_version, 'metadata') and op.create_table_version.metadata:
                        ctv_req.metadata.update(op.create_table_version.metadata)
                    if hasattr(op.create_table_version, 'naming_scheme') and op.create_table_version.naming_scheme is not None:
                        ctv_req.naming_scheme = op.create_table_version.naming_scheme
                    grpc_op.create_table_version.CopyFrom(ctv_req)
                if op.delete_table_versions is not None:
                    dtv_req = BatchDeleteTableVersionsPRequest()
                    dtv_req.id.extend(op.delete_table_versions.id)
                    if hasattr(op.delete_table_versions, 'versions') and op.delete_table_versions.versions is not None:
                        for v in op.delete_table_versions.versions:
                            if isinstance(v, dict):
                                vr = VersionRange()
                                if 'start_version' in v:
                                    vr.start_version = v['start_version']
                                if 'end_version' in v:
                                    vr.end_version = v['end_version']
                                dtv_req.ranges.append(vr)
                            else:
                                vr = VersionRange()
                                vr.start_version = v
                                vr.end_version = v + 1
                                dtv_req.ranges.append(vr)
                    grpc_op.delete_table_versions.CopyFrom(dtv_req)
                if op.deregister_table is not None:
                    drt_req = DeregisterTablePRequest()
                    drt_req.id.extend(op.deregister_table.id)
                    grpc_op.deregister_table.CopyFrom(drt_req)
            grpc_request.catalog = "lance"
            result = self.client.batch_commit_tables(grpc_request)
            return BatchCommitTablesResponse(**result) if isinstance(result, dict) else result
        except Exception as e:
            logger.error(f"Failed to batch commit tables: {e}")
            raise

    def __getstate__(self):
        """Prepare instance for pickling."""
        state = self.__dict__.copy()
        state["_client"] = None
        return state

    def __setstate__(self, state):
        """Restore instance from pickled state."""
        self.__dict__.update(state)

    def close(self):
        """Close the GooseFS client connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
