"""
Unity Catalog namespace implementation for Lance.
"""

import io
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.ipc as ipc

from lance.namespace import LanceNamespace
from lance_namespace_urllib3_client.models import (
    CreateEmptyTableRequest,
    CreateEmptyTableResponse,
    CreateNamespaceRequest,
    CreateNamespaceResponse,
    DeregisterTableRequest,
    DeregisterTableResponse,
    DescribeNamespaceRequest,
    DescribeNamespaceResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    DropNamespaceRequest,
    DropNamespaceResponse,
    ListNamespacesRequest,
    ListNamespacesResponse,
    ListTablesRequest,
    ListTablesResponse,
)

from lance_namespace_impls.rest_client import (
    RestClient,
    RestClientException,
    InternalException,
    InvalidInputException,
    NamespaceAlreadyExistsException,
    NamespaceNotFoundException,
    TableAlreadyExistsException,
    TableNotFoundException,
)

logger = logging.getLogger(__name__)


@dataclass
class UnityNamespaceConfig:
    """Configuration for Unity Catalog namespace."""

    ENDPOINT = "unity.endpoint"
    ROOT = "unity.root"
    AUTH_TOKEN = "unity.auth_token"
    CONNECT_TIMEOUT = "unity.connect_timeout_millis"
    READ_TIMEOUT = "unity.read_timeout_millis"
    MAX_RETRIES = "unity.max_retries"

    endpoint: str
    root: str
    auth_token: Optional[str] = None
    connect_timeout: int = 10000
    read_timeout: int = 300000
    max_retries: int = 3

    def __init__(self, properties: Dict[str, str]):
        self.endpoint = properties.get(self.ENDPOINT)
        if not self.endpoint:
            raise ValueError(f"Required property {self.ENDPOINT} is not set")

        self.root = properties.get(self.ROOT, "/tmp/lance")
        self.auth_token = properties.get(self.AUTH_TOKEN)
        self.connect_timeout = int(properties.get(self.CONNECT_TIMEOUT, "10000"))
        self.read_timeout = int(properties.get(self.READ_TIMEOUT, "300000"))
        self.max_retries = int(properties.get(self.MAX_RETRIES, "3"))

    def get_full_api_url(self) -> str:
        """Get the full API URL with /api/2.1/unity-catalog path."""
        base = self.endpoint.rstrip("/")
        if not base.endswith("/api/2.1/unity-catalog"):
            if base.endswith("/api/2.1"):
                base = f"{base}/unity-catalog"
            else:
                base = f"{base}/api/2.1/unity-catalog"
        return base


@dataclass
class SchemaInfo:
    """Unity schema information."""

    name: str
    catalog_name: str
    comment: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    full_name: Optional[str] = None
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    schema_id: Optional[str] = None


@dataclass
class ColumnInfo:
    """Unity column information."""

    name: str
    type_text: str
    type_json: str
    type_name: str
    position: int
    nullable: bool = True
    comment: Optional[str] = None
    type_precision: Optional[int] = None
    type_scale: Optional[int] = None
    type_interval_type: Optional[str] = None
    partition_index: Optional[int] = None


@dataclass
class TableInfo:
    """Unity table information."""

    name: str
    catalog_name: str
    schema_name: str
    table_type: str
    data_source_format: str
    columns: List[ColumnInfo]
    storage_location: str
    comment: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    table_id: Optional[str] = None
    full_name: Optional[str] = None


@dataclass
class CreateSchema:
    """Request to create a schema."""

    name: str
    catalog_name: str
    properties: Optional[Dict[str, str]] = None


@dataclass
class CreateTable:
    """Request to create a table."""

    name: str
    catalog_name: str
    schema_name: str
    table_type: str
    data_source_format: str
    columns: List[ColumnInfo]
    storage_location: str
    properties: Optional[Dict[str, str]] = None


def _parse_schema_info(data: Dict[str, Any]) -> SchemaInfo:
    """Parse SchemaInfo from response dict."""
    return SchemaInfo(
        name=data.get("name", ""),
        catalog_name=data.get("catalog_name", ""),
        comment=data.get("comment"),
        properties=data.get("properties", {}),
        full_name=data.get("full_name"),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
        schema_id=data.get("schema_id"),
    )


def _parse_table_info(data: Dict[str, Any]) -> TableInfo:
    """Parse TableInfo from response dict."""
    columns_data = data.get("columns", [])
    columns = [ColumnInfo(**col) for col in columns_data]
    return TableInfo(
        name=data.get("name", ""),
        catalog_name=data.get("catalog_name", ""),
        schema_name=data.get("schema_name", ""),
        table_type=data.get("table_type", ""),
        data_source_format=data.get("data_source_format", ""),
        columns=columns,
        storage_location=data.get("storage_location", ""),
        comment=data.get("comment"),
        properties=data.get("properties", {}),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
        table_id=data.get("table_id"),
        full_name=data.get("full_name"),
    )


class UnityNamespace(LanceNamespace):
    """Unity Catalog namespace implementation for Lance."""

    TABLE_TYPE_LANCE = "lance"
    TABLE_TYPE_EXTERNAL = "EXTERNAL"
    MANAGED_BY_KEY = "managed_by"
    TABLE_TYPE_KEY = "table_type"
    VERSION_KEY = "version"

    def __init__(self, **properties):
        """Initialize Unity namespace with configuration properties."""
        self.config = UnityNamespaceConfig(properties)

        headers = {}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"

        self.rest_client = RestClient(
            base_url=self.config.get_full_api_url(),
            headers=headers,
            connect_timeout=self.config.connect_timeout,
            read_timeout=self.config.read_timeout,
            max_retries=self.config.max_retries,
        )

        logger.info(
            f"Initialized Unity namespace with endpoint: {self.config.endpoint}"
        )

    def namespace_id(self) -> str:
        """Return a human-readable unique identifier for this namespace instance."""
        return f"UnityNamespace {{ endpoint: {self.config.endpoint!r} }}"

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        """List namespaces."""
        ns_id = self._parse_identifier(request.id)

        if len(ns_id) > 1:
            raise InvalidInputException(
                f"Expect at most 1-level namespace but get {'.'.join(ns_id)}"
            )

        try:
            namespaces = []

            if len(ns_id) == 0:
                # List all catalogs
                params = {}
                if request.limit:
                    params["max_results"] = str(request.limit)
                if request.page_token:
                    params["page_token"] = request.page_token

                response = self.rest_client.get(
                    "/catalogs", params=params if params else None
                )

                if response and "catalogs" in response:
                    namespaces = [catalog["name"] for catalog in response["catalogs"]]

            elif len(ns_id) == 1:
                # List schemas in a catalog
                catalog = ns_id[0]

                params = {"catalog_name": catalog}
                if request.limit:
                    params["max_results"] = str(request.limit)
                if request.page_token:
                    params["page_token"] = request.page_token

                response = self.rest_client.get("/schemas", params=params)

                if response and "schemas" in response:
                    namespaces = [schema["name"] for schema in response["schemas"]]

            namespaces = sorted(set(namespaces))

            return ListNamespacesResponse(namespaces=namespaces)

        except RestClientException as e:
            if e.is_not_found():
                raise NamespaceNotFoundException(
                    f"Namespace not found: {'.'.join(ns_id)}"
                )
            raise InternalException(f"Failed to list namespaces: {e}")
        except InvalidInputException:
            raise
        except Exception as e:
            raise InternalException(f"Failed to list namespaces: {e}")

    def create_namespace(
        self, request: CreateNamespaceRequest
    ) -> CreateNamespaceResponse:
        """Create a new namespace."""
        ns_id = self._parse_identifier(request.id)

        if len(ns_id) != 2:
            raise InvalidInputException(
                f"Expect a 2-level namespace (catalog.schema) but get {'.'.join(ns_id)}"
            )

        catalog = ns_id[0]
        schema = ns_id[1]

        try:
            create_schema = CreateSchema(
                name=schema, catalog_name=catalog, properties=request.properties
            )

            schema_info = self.rest_client.post(
                "/schemas", create_schema, response_converter=_parse_schema_info
            )

            logger.info(f"Created namespace: {catalog}.{schema}")

            return CreateNamespaceResponse(properties=schema_info.properties)

        except RestClientException as e:
            if e.is_conflict():
                raise NamespaceAlreadyExistsException(
                    f"Namespace already exists: {'.'.join(request.id)}"
                )
            raise InternalException(f"Failed to create namespace: {e}")
        except Exception as e:
            if isinstance(e, (NamespaceAlreadyExistsException, InvalidInputException)):
                raise
            raise InternalException(f"Failed to create namespace: {e}")

    def describe_namespace(
        self, request: DescribeNamespaceRequest
    ) -> DescribeNamespaceResponse:
        """Describe a namespace."""
        ns_id = self._parse_identifier(request.id)

        if len(ns_id) != 2:
            raise InvalidInputException(
                f"Expect a 2-level namespace (catalog.schema) but get {'.'.join(ns_id)}"
            )

        catalog = ns_id[0]
        schema = ns_id[1]

        try:
            full_name = f"{catalog}.{schema}"
            schema_info = self.rest_client.get(
                f"/schemas/{full_name}", response_converter=_parse_schema_info
            )

            return DescribeNamespaceResponse(properties=schema_info.properties)

        except RestClientException as e:
            if e.is_not_found():
                raise NamespaceNotFoundException(
                    f"Namespace not found: {'.'.join(request.id)}"
                )
            raise InternalException(f"Failed to describe namespace: {e}")
        except Exception as e:
            if isinstance(e, (NamespaceNotFoundException, InvalidInputException)):
                raise
            raise InternalException(f"Failed to describe namespace: {e}")

    def drop_namespace(self, request: DropNamespaceRequest) -> DropNamespaceResponse:
        """Drop a namespace."""
        if request.behavior and request.behavior.lower() == "cascade":
            raise InvalidInputException(
                "Cascade behavior is not supported for this implementation"
            )

        ns_id = self._parse_identifier(request.id)

        if len(ns_id) != 2:
            raise InvalidInputException(
                f"Expect a 2-level namespace (catalog.schema) but get {'.'.join(ns_id)}"
            )

        catalog = ns_id[0]
        schema = ns_id[1]

        try:
            full_name = f"{catalog}.{schema}"
            self.rest_client.delete(f"/schemas/{full_name}")
            logger.info(f"Dropped namespace: {full_name}")

            return DropNamespaceResponse(properties={})

        except RestClientException as e:
            if e.is_not_found():
                return DropNamespaceResponse(properties={})
            raise InternalException(f"Failed to drop namespace: {e}")
        except Exception as e:
            if isinstance(e, InvalidInputException):
                raise
            raise InternalException(f"Failed to drop namespace: {e}")

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        """List tables in a namespace."""
        ns_id = self._parse_identifier(request.id)

        if len(ns_id) != 2:
            raise InvalidInputException(
                f"Expect a 2-level namespace (catalog.schema) but get {'.'.join(ns_id)}"
            )

        catalog = ns_id[0]
        schema = ns_id[1]

        try:
            params = {"catalog_name": catalog, "schema_name": schema}
            if request.limit:
                params["max_results"] = str(request.limit)
            if request.page_token:
                params["page_token"] = request.page_token

            response = self.rest_client.get("/tables", params=params)

            tables = []
            if response and "tables" in response:
                for table_data in response["tables"]:
                    if self._is_lance_table(table_data):
                        tables.append(table_data["name"])

            tables = sorted(set(tables))

            return ListTablesResponse(tables=tables)

        except (NamespaceNotFoundException, InvalidInputException):
            raise
        except Exception as e:
            raise InternalException(f"Failed to list tables: {e}")

    def create_empty_table(
        self, request: CreateEmptyTableRequest
    ) -> CreateEmptyTableResponse:
        """Create an empty table (metadata only operation)."""
        table_id = self._parse_identifier(request.id)

        if len(table_id) != 3:
            raise InvalidInputException(
                f"Expect a 3-level table identifier (catalog.schema.table) but get {'.'.join(table_id)}"
            )

        catalog = table_id[0]
        schema = table_id[1]
        table = table_id[2]

        try:
            table_path = request.location
            if not table_path:
                table_path = f"{self.config.root}/{catalog}/{schema}/{table}"

            columns = [
                ColumnInfo(
                    name="__placeholder_id",
                    type_text="LONG",
                    type_json='{"type":"long"}',
                    type_name="LONG",
                    position=0,
                    nullable=True,
                )
            ]

            properties = {
                self.TABLE_TYPE_KEY: self.TABLE_TYPE_LANCE,
                self.MANAGED_BY_KEY: "catalog",
            }

            create_table = CreateTable(
                name=table,
                catalog_name=catalog,
                schema_name=schema,
                table_type=self.TABLE_TYPE_EXTERNAL,
                data_source_format="TEXT",
                columns=columns,
                storage_location=table_path,
                properties=properties,
            )

            self.rest_client.post(
                "/tables", create_table, response_converter=_parse_table_info
            )

            logger.info(f"Created empty table: {catalog}.{schema}.{table}")

            return CreateEmptyTableResponse(location=table_path)

        except RestClientException as e:
            if e.is_conflict():
                raise TableAlreadyExistsException(
                    f"Table already exists: {'.'.join(request.id)}"
                )
            raise InternalException(f"Failed to create empty table: {e}")
        except Exception as e:
            if isinstance(e, (TableAlreadyExistsException, InvalidInputException)):
                raise
            raise InternalException(f"Failed to create empty table: {e}")

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        """Describe a table."""
        if request.load_detailed_metadata:
            raise InvalidInputException(
                "load_detailed_metadata=true is not supported for this implementation"
            )

        table_id = self._parse_identifier(request.id)

        if len(table_id) != 3:
            raise InvalidInputException(
                f"Expect a 3-level table identifier (catalog.schema.table) but get {'.'.join(table_id)}"
            )

        catalog = table_id[0]
        schema = table_id[1]
        table = table_id[2]

        try:
            full_name = f"{catalog}.{schema}.{table}"
            table_info = self.rest_client.get(
                f"/tables/{full_name}", response_converter=_parse_table_info
            )

            if not self._is_lance_table_info(table_info):
                raise InvalidInputException(
                    f"Table {'.'.join(request.id)} is not a Lance table"
                )

            return DescribeTableResponse(
                location=table_info.storage_location,
                storage_options=table_info.properties,
            )

        except RestClientException as e:
            if e.is_not_found():
                raise TableNotFoundException(f"Table not found: {'.'.join(request.id)}")
            raise InternalException(f"Failed to describe table: {e}")
        except Exception as e:
            if isinstance(
                e,
                (
                    TableNotFoundException,
                    NamespaceNotFoundException,
                    InvalidInputException,
                ),
            ):
                raise
            raise InternalException(f"Failed to describe table: {e}")

    def deregister_table(
        self, request: DeregisterTableRequest
    ) -> DeregisterTableResponse:
        """Deregister a table (remove from catalog without deleting data)."""
        table_id = self._parse_identifier(request.id)

        if len(table_id) != 3:
            raise InvalidInputException(
                f"Expect a 3-level table identifier (catalog.schema.table) but get {'.'.join(table_id)}"
            )

        catalog = table_id[0]
        schema = table_id[1]
        table = table_id[2]

        try:
            full_name = f"{catalog}.{schema}.{table}"

            table_info = self.rest_client.get(
                f"/tables/{full_name}", response_converter=_parse_table_info
            )

            if not self._is_lance_table_info(table_info):
                raise InvalidInputException(
                    f"Table {'.'.join(request.id)} is not a Lance table"
                )

            location = table_info.storage_location
            self.rest_client.delete(f"/tables/{full_name}")

            logger.info(f"Deregistered table: {full_name}")

            return DeregisterTableResponse(location=location)

        except RestClientException as e:
            if e.is_not_found():
                raise TableNotFoundException(f"Table not found: {'.'.join(request.id)}")
            raise InternalException(f"Failed to deregister table: {e}")
        except Exception as e:
            if isinstance(e, (TableNotFoundException, InvalidInputException)):
                raise
            raise InternalException(f"Failed to deregister table: {e}")

    def close(self):
        """Close the namespace connection."""
        if self.rest_client:
            self.rest_client.close()

    def _parse_identifier(self, identifier: List[str]) -> List[str]:
        """Parse identifier list."""
        return identifier if identifier else []

    def _is_lance_table(self, table_data: Dict[str, Any]) -> bool:
        """Check if a table dictionary represents a Lance table."""
        if not table_data or "properties" not in table_data:
            return False
        properties = table_data.get("properties", {})
        table_type = properties.get(self.TABLE_TYPE_KEY)
        return table_type and table_type.lower() == self.TABLE_TYPE_LANCE.lower()

    def _is_lance_table_info(self, table_info: TableInfo) -> bool:
        """Check if a TableInfo represents a Lance table."""
        if not table_info or not table_info.properties:
            return False
        table_type = table_info.properties.get(self.TABLE_TYPE_KEY)
        return table_type and table_type.lower() == self.TABLE_TYPE_LANCE.lower()

    def _extract_schema_from_ipc(self, ipc_data: bytes) -> pa.Schema:
        """Extract Arrow schema from IPC stream."""
        try:
            reader = ipc.open_stream(io.BytesIO(ipc_data))
            return reader.schema
        except Exception as e:
            raise InvalidInputException(f"Invalid Arrow IPC stream: {e}")

    def _convert_arrow_schema_to_unity_columns(
        self, arrow_schema: pa.Schema
    ) -> List[ColumnInfo]:
        """Convert Arrow schema to Unity column definitions."""
        columns = []
        for i, arrow_field in enumerate(arrow_schema):
            unity_type = self._convert_arrow_type_to_unity_type(arrow_field.type)
            unity_type_json = self._convert_arrow_type_to_unity_type_json(
                arrow_field.type
            )

            column = ColumnInfo(
                name=arrow_field.name,
                type_text=unity_type,
                type_json=unity_type_json,
                type_name=unity_type,
                position=i,
                nullable=arrow_field.nullable,
            )
            columns.append(column)

        return columns

    def _convert_arrow_type_to_unity_type(self, arrow_type: pa.DataType) -> str:
        """Convert Arrow type to Unity type string."""
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "STRING"
        elif pa.types.is_int32(arrow_type):
            return "INT"
        elif pa.types.is_int64(arrow_type):
            return "LONG"
        elif pa.types.is_float32(arrow_type):
            return "FLOAT"
        elif pa.types.is_float64(arrow_type):
            return "DOUBLE"
        elif pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        elif pa.types.is_date(arrow_type):
            return "DATE"
        elif pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        else:
            return "STRING"

    def _convert_arrow_type_to_unity_type_json(self, arrow_type: pa.DataType) -> str:
        """Convert Arrow type to Unity type JSON string."""
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return '{"type":"string"}'
        elif pa.types.is_int32(arrow_type):
            return '{"type":"integer"}'
        elif pa.types.is_int64(arrow_type):
            return '{"type":"long"}'
        elif pa.types.is_float32(arrow_type):
            return '{"type":"float"}'
        elif pa.types.is_float64(arrow_type):
            return '{"type":"double"}'
        elif pa.types.is_boolean(arrow_type):
            return '{"type":"boolean"}'
        elif pa.types.is_date(arrow_type):
            return '{"type":"date"}'
        elif pa.types.is_timestamp(arrow_type):
            return '{"type":"timestamp"}'
        else:
            return '{"type":"string"}'
