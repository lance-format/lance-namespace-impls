"""
Lance Hive2 Namespace implementation using Hive Metastore.

This module provides integration with Apache Hive Metastore for managing Lance tables.
Lance tables are registered as external tables in Hive with specific metadata properties
to identify them as Lance format.

Installation:
    pip install 'lance-namespace[hive2]'

Usage:
    from lance_namespace import connect

    # Connect to Hive Metastore
    namespace = connect("hive2", {
        "uri": "thrift://localhost:9083",
        "root": "/my/dir",  # Or "s3://bucket/prefix"
        "ugi": "user:group1,group2"  # Optional user/group info
    })

    # List databases
    from lance_namespace import ListNamespacesRequest
    response = namespace.list_namespaces(ListNamespacesRequest())

Configuration Properties:
    uri (str): Hive Metastore Thrift URI (e.g., "thrift://localhost:9083")
    root (str): Storage root location of the lakehouse on Hive catalog (default: current working directory)
    ugi (str): Optional User Group Information for authentication (format: "user:group1,group2")
    client.pool-size (int): Size of the HMS client connection pool (default: 3)
    storage.* (str): Additional storage configurations to access table
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

# Table properties used by Lance (per hive.md specification)
TABLE_TYPE_KEY = "table_type"  # Case insensitive
LANCE_TABLE_FORMAT = "lance"  # Case insensitive
MANAGED_BY_KEY = "managed_by"  # Case insensitive, values: "storage" or "impl"
VERSION_KEY = "version"  # Numeric version number
EXTERNAL_TABLE = "EXTERNAL_TABLE"


class HiveMetastoreClientWrapper:
    """Helper class to manage Hive Metastore client connections."""

    def __init__(self, uri: str, ugi: Optional[str] = None):
        if not HIVE_AVAILABLE:
            raise ImportError(
                "Hive dependencies not installed. Please install with: "
                "pip install 'lance-namespace[hive2]'"
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


class Hive2Namespace(LanceNamespace):
    """Lance Hive2 Namespace implementation using Hive Metastore."""

    def __init__(self, **properties):
        """Initialize the Hive2 namespace.

        Args:
            uri: The Hive Metastore URI (e.g., "thrift://localhost:9083")
            root: Storage root location of the lakehouse on Hive catalog (optional)
            ugi: User Group Information for authentication (optional, format: "user:group1,group2")
            client.pool-size: Size of the HMS client connection pool (optional, default: 3)
            storage.*: Additional storage configurations to access table
            **properties: Additional configuration properties
        """
        if not HIVE_AVAILABLE:
            raise ImportError(
                "Hive dependencies not installed. Please install with: "
                "pip install 'lance-namespace[hive2]'"
            )

        self.uri = properties.get("uri", "thrift://localhost:9083")
        self.ugi = properties.get("ugi")
        self.root = properties.get("root", os.getcwd())
        self.pool_size = int(properties.get("client.pool-size", "3"))
        # Extract storage properties
        self.storage_properties = {
            k[8:]: v for k, v in properties.items() if k.startswith("storage.")
        }

        # Store properties for pickling support
        self._properties = properties.copy()

        # Lazy initialization to support pickling
        self._client = None

    def namespace_id(self) -> str:
        """Return a human-readable unique identifier for this namespace instance."""
        return f"Hive2Namespace {{ uri: {self.uri!r} }}"

    @property
    def client(self):
        """Get the Hive client, initializing it if necessary."""
        if self._client is None:
            self._client = HiveMetastoreClientWrapper(self.uri, self.ugi)
        return self._client

    def _normalize_identifier(self, identifier: List[str]) -> tuple:
        """Normalize identifier to (database, table) tuple."""
        if len(identifier) == 1:
            return ("default", identifier[0])
        elif len(identifier) == 2:
            return (identifier[0], identifier[1])
        else:
            raise ValueError(f"Invalid identifier: {identifier}")

    def _is_root_namespace(self, identifier: Optional[List[str]]) -> bool:
        """Check if the identifier refers to the root namespace."""
        return not identifier or len(identifier) == 0

    def _get_table_location(self, database: str, table: str) -> str:
        """Get the location for a table."""
        return os.path.join(self.root, f"{database}.db", table)

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        """List all databases in the Hive Metastore."""
        try:
            # Only list namespaces if we're at the root level
            if not self._is_root_namespace(request.id):
                # Non-root namespaces don't have children in Hive2
                return ListNamespacesResponse(namespaces=[])

            with self.client as client:
                databases = client.get_all_databases()
                # Return just database names as strings (excluding default)
                namespaces = [db for db in databases if db != "default"]

                return ListNamespacesResponse(namespaces=namespaces)
        except Exception as e:
            logger.error(f"Failed to list namespaces: {e}")
            raise

    def describe_namespace(
        self, request: DescribeNamespaceRequest
    ) -> DescribeNamespaceResponse:
        """Describe a database in the Hive Metastore."""
        try:
            # Handle root namespace
            if self._is_root_namespace(request.id):
                properties = {
                    "location": self.root,
                    "description": "Root namespace (Hive Metastore)",
                }
                if self.ugi:
                    properties["ugi"] = self.ugi
                return DescribeNamespaceResponse(properties=properties)

            if len(request.id) != 1:
                raise ValueError(f"Invalid namespace identifier: {request.id}")

            database_name = request.id[0]

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
        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Namespace {request.id} does not exist")
            logger.error(f"Failed to describe namespace {request.id}: {e}")
            raise

    def create_namespace(
        self, request: CreateNamespaceRequest
    ) -> CreateNamespaceResponse:
        """Create a new database in the Hive Metastore."""
        try:
            # Cannot create root namespace
            if self._is_root_namespace(request.id):
                raise ValueError("Root namespace already exists")

            if len(request.id) != 1:
                raise ValueError(f"Invalid namespace identifier: {request.id}")

            database_name = request.id[0]

            # Create database object
            if not HiveDatabase:
                raise ImportError("Hive dependencies not available")

            props = request.properties or {}
            database = HiveDatabase()
            database.name = database_name
            database.description = props.get("comment", "")
            database.ownerName = props.get("owner", os.getenv("USER", ""))
            database.locationUri = props.get(
                "location", os.path.join(self.root, f"{database_name}.db")
            )
            database.parameters = {
                k: v
                for k, v in props.items()
                if k not in ["comment", "owner", "location"]
            }

            with self.client as client:
                client.create_database(database)

            return CreateNamespaceResponse()
        except Exception as e:
            if AlreadyExistsException and isinstance(e, AlreadyExistsException):
                raise ValueError(f"Namespace {request.id} already exists")
            logger.error(f"Failed to create namespace {request.id}: {e}")
            raise

    def drop_namespace(self, request: DropNamespaceRequest) -> DropNamespaceResponse:
        """Drop a database from the Hive Metastore. Only RESTRICT mode is supported."""
        if request.behavior and request.behavior.lower() == "cascade":
            raise InvalidInputException(
                "Cascade behavior is not supported for this implementation"
            )

        try:
            # Cannot drop root namespace
            if self._is_root_namespace(request.id):
                raise ValueError("Cannot drop root namespace")

            if len(request.id) != 1:
                raise ValueError(f"Invalid namespace identifier: {request.id}")

            database_name = request.id[0]

            with self.client as client:
                # Check if database is empty (RESTRICT mode only)
                tables = client.get_all_tables(database_name)
                if tables:
                    raise ValueError(f"Namespace {request.id} is not empty")

                # Drop database
                client.drop_database(database_name, deleteData=True, cascade=False)

            return DropNamespaceResponse()
        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Namespace {request.id} does not exist")
            logger.error(f"Failed to drop namespace {request.id}: {e}")
            raise

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        """List tables in a database."""
        try:
            # Root namespace has no tables
            if self._is_root_namespace(request.id):
                return ListTablesResponse(tables=[])

            if len(request.id) != 1:
                raise ValueError(f"Invalid namespace identifier: {request.id}")

            database_name = request.id[0]

            with self.client as client:
                table_names = client.get_all_tables(database_name)

                # Filter for Lance tables if needed
                tables = []
                for table_name in table_names:
                    try:
                        table = client.get_table(database_name, table_name)
                        # Check if it's a Lance table (case insensitive)
                        if table.parameters:
                            table_type = table.parameters.get(
                                TABLE_TYPE_KEY, ""
                            ).lower()
                            if table_type == LANCE_TABLE_FORMAT:
                                # Return just table name, not full identifier
                                tables.append(table_name)
                    except Exception:
                        # Skip tables we can't read
                        continue

                return ListTablesResponse(tables=tables)
        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Namespace {request.id} does not exist")
            logger.error(f"Failed to list tables in namespace {request.id}: {e}")
            raise

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        """Describe a table in the Hive Metastore.

        Only load_detailed_metadata=false is supported. Returns location and storage_options only.
        """
        if request.load_detailed_metadata:
            raise ValueError(
                "load_detailed_metadata=true is not supported for this implementation"
            )

        try:
            database, table_name = self._normalize_identifier(request.id)

            with self.client as client:
                table = client.get_table(database, table_name)

                # Check if it's a Lance table (case insensitive)
                if not table.parameters:
                    raise ValueError(f"Table {request.id} is not a Lance table")
                table_type = table.parameters.get(TABLE_TYPE_KEY, "").lower()
                if table_type != LANCE_TABLE_FORMAT:
                    raise ValueError(f"Table {request.id} is not a Lance table")

                # Get table location
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
        """Deregister a table from the Hive Metastore without deleting data."""
        try:
            database, table_name = self._normalize_identifier(request.id)

            with self.client as client:
                # Get table to check if it's a Lance table
                table = client.get_table(database, table_name)

                # Check if it's a Lance table (case insensitive)
                if not table.parameters:
                    raise ValueError(f"Table {request.id} is not a Lance table")
                table_type = table.parameters.get(TABLE_TYPE_KEY, "").lower()
                if table_type != LANCE_TABLE_FORMAT:
                    raise ValueError(f"Table {request.id} is not a Lance table")

                location = table.sd.location if table.sd else None

                # Drop the table metadata only (don't delete data)
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
        """Create an empty table (metadata only) in Hive metastore."""
        try:
            database, table_name = self._normalize_identifier(request.id)

            # Determine table location
            location = request.location
            if not location:
                location = self._get_table_location(database, table_name)

            # Create a minimal schema for Hive (placeholder schema)
            if not FieldSchema:
                raise ImportError("Hive dependencies not available")

            fields = [
                FieldSchema(
                    name="__placeholder_id",
                    type="bigint",
                    comment="Placeholder column for empty table",
                )
            ]

            # Create Hive table metadata without creating actual Lance dataset
            storage_descriptor = StorageDescriptor(
                cols=fields,
                location=location,
                inputFormat="org.apache.hadoop.mapred.TextInputFormat",
                outputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                serdeInfo=SerDeInfo(
                    serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                ),
            )

            # Set table parameters to identify it as Lance table
            parameters = {
                TABLE_TYPE_KEY: "LANCE",
                MANAGED_BY_KEY: "storage",
                "empty_table": "true",  # Mark as empty table
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

            # Create table in Hive
            with self.client as client:
                client.create_table(hive_table)

            return CreateEmptyTableResponse(location=location)

        except AlreadyExistsException:
            raise ValueError(f"Table {request.id} already exists")
        except Exception as e:
            logger.error(f"Failed to create empty table {request.id}: {e}")
            raise

    def __getstate__(self):
        """Prepare instance for pickling by excluding unpickleable objects."""
        state = self.__dict__.copy()
        # Remove the unpickleable Hive client
        state["_client"] = None
        return state

    def __setstate__(self, state):
        """Restore instance from pickled state."""
        self.__dict__.update(state)
        # The Hive client will be re-initialized lazily via the property

    def close(self):
        """Close the Hive Metastore client connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
