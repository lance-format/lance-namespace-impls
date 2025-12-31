"""
Lance Hive3 Namespace implementation using Hive 3.x Metastore.

This module provides integration with Apache Hive 3.x Metastore for managing Lance tables.
Hive3 supports a 3-level namespace hierarchy: catalog > database > table.

Installation:
    pip install 'lance-namespace[hive3]'

Usage:
    from lance_namespace import connect

    # Connect to Hive 3.x Metastore
    namespace = connect("hive3", {
        "uri": "thrift://localhost:9083",
        "root": "/my/dir",  # Or "s3://bucket/prefix"
        "ugi": "user:group1,group2"  # Optional user/group info
    })

    # List catalogs (root level)
    from lance_namespace import ListNamespacesRequest
    response = namespace.list_namespaces(ListNamespacesRequest())

    # List databases in a catalog
    response = namespace.list_namespaces(ListNamespacesRequest(id=["my_catalog"]))

Configuration Properties:
    uri (str): Hive Metastore Thrift URI (e.g., "thrift://localhost:9083")
    root (str): Storage root location of the lakehouse (default: current working directory)
    ugi (str): Optional User Group Information for authentication (format: "user:group1,group2")
    client.pool-size (int): Size of the HMS client connection pool (default: 3)
    storage.* (str): Additional storage configurations
"""

from typing import List, Optional
from urllib.parse import urlparse
import os
import logging

try:
    from hive_metastore_client import HiveMetastoreClient as Client
    from thrift_files.libraries.thrift_hive_metastore_client.ttypes import (
        Database as HiveDatabase,
        Table as HiveTable,
        StorageDescriptor,
        SerDeInfo,
        FieldSchema,
        NoSuchObjectException,
        AlreadyExistsException,
        InvalidOperationException,
        MetaException,
    )

    HIVE_AVAILABLE = True
except ImportError:
    HIVE_AVAILABLE = False
    Client = None
    HiveDatabase = None
    HiveTable = None
    StorageDescriptor = None
    SerDeInfo = None
    FieldSchema = None
    NoSuchObjectException = None
    AlreadyExistsException = None
    InvalidOperationException = None
    MetaException = None

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
    CreateEmptyTableRequest,
    CreateEmptyTableResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    DeregisterTableRequest,
    DeregisterTableResponse,
)

from lance_namespace_impls.rest_client import InvalidInputException

logger = logging.getLogger(__name__)

TABLE_TYPE_KEY = "table_type"
LANCE_TABLE_FORMAT = "lance"
MANAGED_BY_KEY = "managed_by"
VERSION_KEY = "version"
EXTERNAL_TABLE = "EXTERNAL_TABLE"
DEFAULT_CATALOG = "hive"


class Hive3MetastoreClientWrapper:
    """Helper class to manage Hive 3.x Metastore client connections."""

    def __init__(self, uri: str, ugi: Optional[str] = None):
        if not HIVE_AVAILABLE:
            raise ImportError(
                "Hive dependencies not installed. Please install with: "
                "pip install 'lance-namespace[hive3]'"
            )

        self._uri = uri
        self._ugi = ugi.split(":") if ugi else None
        url_parts = urlparse(self._uri)
        self._host = url_parts.hostname or "localhost"
        self._port = url_parts.port or 9083
        self._client = None

    def __enter__(self):
        """Enter context manager."""
        self._client = Client(host=self._host, port=self._port)
        self._client.open()
        if self._ugi:
            self._client.set_ugi(*self._ugi)
        return self._client

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        if self._client:
            self._client.close()
            self._client = None

    def close(self):
        """Close the client connection."""
        if self._client:
            self._client.close()
            self._client = None


class Hive3Namespace(LanceNamespace):
    """Lance Hive3 Namespace implementation using Hive 3.x Metastore.

    Supports 3-level namespace hierarchy: catalog > database > table.
    """

    def __init__(self, **properties):
        """Initialize the Hive3 namespace.

        Args:
            uri: The Hive Metastore URI (e.g., "thrift://localhost:9083")
            root: Storage root location (optional)
            ugi: User Group Information for authentication (optional)
            client.pool-size: Size of the HMS client connection pool (optional, default: 3)
            storage.*: Additional storage configurations
            **properties: Additional configuration properties
        """
        if not HIVE_AVAILABLE:
            raise ImportError(
                "Hive dependencies not installed. Please install with: "
                "pip install 'lance-namespace[hive3]'"
            )

        self.uri = properties.get("uri", "thrift://localhost:9083")
        self.ugi = properties.get("ugi")
        self.root = properties.get("root", os.getcwd())
        self.pool_size = int(properties.get("client.pool-size", "3"))
        self.storage_properties = {
            k[8:]: v for k, v in properties.items() if k.startswith("storage.")
        }

        self._properties = properties.copy()
        self._client = None

    def namespace_id(self) -> str:
        """Return a human-readable unique identifier for this namespace instance."""
        return f"Hive3Namespace {{ uri: {self.uri!r} }}"

    @property
    def client(self):
        """Get the Hive client, initializing it if necessary."""
        if self._client is None:
            self._client = Hive3MetastoreClientWrapper(self.uri, self.ugi)
        return self._client

    def _normalize_identifier(self, identifier: List[str]) -> tuple:
        """Normalize identifier to (catalog, database, table) tuple."""
        if len(identifier) == 1:
            return (DEFAULT_CATALOG, "default", identifier[0])
        elif len(identifier) == 2:
            return (DEFAULT_CATALOG, identifier[0], identifier[1])
        elif len(identifier) == 3:
            return (identifier[0], identifier[1], identifier[2])
        else:
            raise ValueError(f"Invalid identifier: {identifier}")

    def _is_root_namespace(self, identifier: Optional[List[str]]) -> bool:
        """Check if the identifier refers to the root namespace."""
        return not identifier or len(identifier) == 0

    def _get_table_location(self, catalog: str, database: str, table: str) -> str:
        """Get the location for a table."""
        return os.path.join(self.root, database, f"{table}.lance")

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        """List namespaces at the given level.

        - Root level: lists catalogs
        - Catalog level: lists databases in that catalog
        """
        try:
            ns_id = request.id if request.id else []

            if self._is_root_namespace(ns_id):
                # List catalogs
                with self.client as client:
                    # Try to get catalogs if supported (Hive 3.x)
                    try:
                        catalogs = (
                            client.get_catalogs().names
                            if hasattr(client, "get_catalogs")
                            else []
                        )
                    except Exception:
                        # Fall back to default catalog
                        catalogs = [DEFAULT_CATALOG]
                    return ListNamespacesResponse(namespaces=catalogs)

            elif len(ns_id) == 1:
                # List databases in catalog
                # Note: Hive 2.x Metastore API doesn't support catalog operations,
                # so we ignore the catalog name and list all databases
                _catalog = ns_id[0].lower()  # noqa: F841
                with self.client as client:
                    try:
                        databases = client.get_all_databases()
                    except Exception:
                        databases = []
                    # Exclude 'default' database from list
                    namespaces = [db for db in databases if db != "default"]
                    return ListNamespacesResponse(namespaces=namespaces)

            else:
                # 2+ level namespaces don't have children
                return ListNamespacesResponse(namespaces=[])

        except Exception as e:
            logger.error(f"Failed to list namespaces: {e}")
            raise

    def describe_namespace(
        self, request: DescribeNamespaceRequest
    ) -> DescribeNamespaceResponse:
        """Describe a namespace (catalog or database)."""
        try:
            if self._is_root_namespace(request.id):
                properties = {
                    "location": self.root,
                    "description": "Root namespace (Hive 3.x Metastore)",
                }
                if self.ugi:
                    properties["ugi"] = self.ugi
                return DescribeNamespaceResponse(properties=properties)

            if len(request.id) == 1:
                # Describe catalog
                catalog_name = request.id[0].lower()
                properties = {
                    "description": f"Catalog: {catalog_name}",
                    "catalog.location.uri": os.path.join(self.root, catalog_name),
                }
                return DescribeNamespaceResponse(properties=properties)

            elif len(request.id) == 2:
                # Describe database
                catalog_name = request.id[0].lower()
                database_name = request.id[1].lower()

                with self.client as client:
                    database = client.get_database(database_name)

                    properties = {}
                    if database.description:
                        properties["comment"] = database.description
                    if database.ownerName:
                        properties["owner"] = database.ownerName
                    if database.locationUri:
                        properties["location"] = database.locationUri
                    if database.parameters:
                        properties.update(database.parameters)

                    return DescribeNamespaceResponse(properties=properties)
            else:
                raise ValueError(f"Invalid namespace identifier: {request.id}")

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Namespace {request.id} does not exist")
            logger.error(f"Failed to describe namespace {request.id}: {e}")
            raise

    def create_namespace(
        self, request: CreateNamespaceRequest
    ) -> CreateNamespaceResponse:
        """Create a new namespace (catalog or database)."""
        try:
            if self._is_root_namespace(request.id):
                raise ValueError("Root namespace already exists")

            mode = request.mode.lower() if request.mode else "create"

            if len(request.id) == 1:
                # Create catalog (Hive 3.x)
                # Note: Python Hive client may not support catalog creation
                catalog_name = request.id[0].lower()
                logger.warning(f"Catalog creation may not be supported: {catalog_name}")
                return CreateNamespaceResponse()

            elif len(request.id) == 2:
                # Create database
                catalog_name = request.id[0].lower()
                database_name = request.id[1].lower()

                if not HiveDatabase:
                    raise ImportError("Hive dependencies not available")

                database = HiveDatabase()
                database.name = database_name
                database.description = (
                    request.properties.get("comment", "") if request.properties else ""
                )
                database.ownerName = (
                    request.properties.get("owner", os.getenv("USER", ""))
                    if request.properties
                    else os.getenv("USER", "")
                )
                database.locationUri = (
                    request.properties.get(
                        "location", os.path.join(self.root, database_name)
                    )
                    if request.properties
                    else os.path.join(self.root, database_name)
                )

                if request.properties:
                    database.parameters = {
                        k: v
                        for k, v in request.properties.items()
                        if k not in ["comment", "owner", "location"]
                    }

                with self.client as client:
                    try:
                        client.create_database(database)
                    except AlreadyExistsException:
                        if mode == "create":
                            raise ValueError(f"Namespace {request.id} already exists")
                        elif mode in ("exist_ok", "existok"):
                            pass  # OK to exist
                        elif mode == "overwrite":
                            client.drop_database(
                                database_name, deleteData=True, cascade=True
                            )
                            client.create_database(database)

                return CreateNamespaceResponse()
            else:
                raise ValueError(f"Invalid namespace identifier: {request.id}")

        except Exception as e:
            if AlreadyExistsException and isinstance(e, AlreadyExistsException):
                raise ValueError(f"Namespace {request.id} already exists")
            logger.error(f"Failed to create namespace {request.id}: {e}")
            raise

    def drop_namespace(self, request: DropNamespaceRequest) -> DropNamespaceResponse:
        """Drop a namespace (catalog or database). Only RESTRICT mode is supported."""
        if request.behavior and request.behavior.lower() == "cascade":
            raise InvalidInputException(
                "Cascade behavior is not supported for this implementation"
            )

        try:
            if self._is_root_namespace(request.id):
                raise ValueError("Cannot drop root namespace")

            if len(request.id) == 1:
                # Drop catalog (Hive 3.x)
                catalog_name = request.id[0].lower()
                logger.warning(f"Catalog drop may not be supported: {catalog_name}")
                return DropNamespaceResponse()

            elif len(request.id) == 2:
                # Drop database
                database_name = request.id[1].lower()

                with self.client as client:
                    # Check if database is empty (RESTRICT mode only)
                    tables = client.get_all_tables(database_name)
                    if tables:
                        raise ValueError(f"Namespace {request.id} is not empty")

                    client.drop_database(database_name, deleteData=True, cascade=False)

                return DropNamespaceResponse()
            else:
                raise ValueError(f"Invalid namespace identifier: {request.id}")

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Namespace {request.id} does not exist")
            logger.error(f"Failed to drop namespace {request.id}: {e}")
            raise

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        """List tables in a database."""
        try:
            if self._is_root_namespace(request.id) or len(request.id) < 2:
                return ListTablesResponse(tables=[])

            # Note: Hive 2.x Metastore API doesn't support catalog operations,
            # so we ignore the catalog name
            _catalog_name = request.id[0].lower()  # noqa: F841
            database_name = request.id[1].lower()

            with self.client as client:
                table_names = client.get_all_tables(database_name)

                # Filter for Lance tables
                tables = []
                for table_name in table_names:
                    try:
                        table = client.get_table(database_name, table_name)
                        if table.parameters:
                            table_type = table.parameters.get(
                                TABLE_TYPE_KEY, ""
                            ).lower()
                            if table_type == LANCE_TABLE_FORMAT:
                                tables.append(table_name)
                    except Exception:
                        continue

                return ListTablesResponse(tables=tables)

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Namespace {request.id} does not exist")
            logger.error(f"Failed to list tables in namespace {request.id}: {e}")
            raise

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        """Describe a table.

        Only load_detailed_metadata=false is supported. Returns location and storage_options only.
        """
        if request.load_detailed_metadata:
            raise ValueError(
                "load_detailed_metadata=true is not supported for this implementation"
            )

        try:
            catalog, database, table_name = self._normalize_identifier(request.id)

            with self.client as client:
                table = client.get_table(database, table_name)

                if not table.parameters:
                    raise ValueError(f"Table {request.id} is not a Lance table")
                table_type = table.parameters.get(TABLE_TYPE_KEY, "").lower()
                if table_type != LANCE_TABLE_FORMAT:
                    raise ValueError(f"Table {request.id} is not a Lance table")

                location = table.sd.location if table.sd else None
                if not location:
                    raise ValueError(f"Table {request.id} has no location")

                return DescribeTableResponse(
                    location=location, storage_options=self.storage_properties
                )

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Table {request.id} does not exist")
            logger.error(f"Failed to describe table {request.id}: {e}")
            raise

    def deregister_table(
        self, request: DeregisterTableRequest
    ) -> DeregisterTableResponse:
        """Deregister a table without deleting data."""
        try:
            catalog, database, table_name = self._normalize_identifier(request.id)

            with self.client as client:
                table = client.get_table(database, table_name)

                if not table.parameters:
                    raise ValueError(f"Table {request.id} is not a Lance table")
                table_type = table.parameters.get(TABLE_TYPE_KEY, "").lower()
                if table_type != LANCE_TABLE_FORMAT:
                    raise ValueError(f"Table {request.id} is not a Lance table")

                location = table.sd.location if table.sd else None

                client.drop_table(database, table_name, deleteData=False)

                return DeregisterTableResponse(location=location)

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Table {request.id} does not exist")
            logger.error(f"Failed to deregister table {request.id}: {e}")
            raise

    def create_empty_table(
        self, request: CreateEmptyTableRequest
    ) -> CreateEmptyTableResponse:
        """Create an empty table (metadata only)."""
        try:
            catalog, database, table_name = self._normalize_identifier(request.id)

            location = request.location
            if not location:
                location = self._get_table_location(catalog, database, table_name)

            if not FieldSchema:
                raise ImportError("Hive dependencies not available")

            fields = [
                FieldSchema(
                    name="__placeholder_id", type="bigint", comment="Placeholder column"
                )
            ]

            storage_descriptor = StorageDescriptor(
                cols=fields,
                location=location,
                inputFormat="org.apache.hadoop.mapred.TextInputFormat",
                outputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                serdeInfo=SerDeInfo(
                    serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                ),
            )

            parameters = {
                TABLE_TYPE_KEY: LANCE_TABLE_FORMAT,
                MANAGED_BY_KEY: "storage",
                "empty_table": "true",
            }

            if hasattr(request, "properties") and request.properties:
                parameters.update(request.properties)

            hive_table = HiveTable(
                tableName=table_name,
                dbName=database,
                sd=storage_descriptor,
                parameters=parameters,
                tableType="EXTERNAL_TABLE",
            )

            with self.client as client:
                client.create_table(hive_table)

            return CreateEmptyTableResponse(location=location)

        except AlreadyExistsException:
            raise ValueError(f"Table {request.id} already exists")
        except Exception as e:
            logger.error(f"Failed to create empty table {request.id}: {e}")
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
        """Close the Hive Metastore client connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
