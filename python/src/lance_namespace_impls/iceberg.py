"""
Iceberg REST Catalog namespace implementation for Lance.
"""

import logging
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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
    NamespaceExistsRequest,
    TableExistsRequest,
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

NAMESPACE_SEPARATOR = "\x1f"


@dataclass
class IcebergNamespaceConfig:
    """Configuration for Iceberg REST Catalog namespace."""

    ENDPOINT = "iceberg.endpoint"
    WAREHOUSE = "iceberg.warehouse"
    PREFIX = "iceberg.prefix"
    AUTH_TOKEN = "iceberg.auth_token"
    CREDENTIAL = "iceberg.credential"
    CONNECT_TIMEOUT = "iceberg.connect_timeout_millis"
    READ_TIMEOUT = "iceberg.read_timeout_millis"
    MAX_RETRIES = "iceberg.max_retries"
    ROOT = "iceberg.root"

    endpoint: str
    warehouse: Optional[str] = None
    prefix: str = ""
    auth_token: Optional[str] = None
    credential: Optional[str] = None
    connect_timeout: int = 10000
    read_timeout: int = 30000
    max_retries: int = 3
    root: str = "/tmp/lance"

    def __init__(self, properties: Dict[str, str]):
        self.endpoint = properties.get(self.ENDPOINT)
        if not self.endpoint:
            raise ValueError(f"Required property {self.ENDPOINT} is not set")

        self.warehouse = properties.get(self.WAREHOUSE)
        self.prefix = properties.get(self.PREFIX, "")
        self.auth_token = properties.get(self.AUTH_TOKEN)
        self.credential = properties.get(self.CREDENTIAL)
        self.connect_timeout = int(properties.get(self.CONNECT_TIMEOUT, "10000"))
        self.read_timeout = int(properties.get(self.READ_TIMEOUT, "30000"))
        self.max_retries = int(properties.get(self.MAX_RETRIES, "3"))
        self.root = properties.get(self.ROOT, "/tmp/lance")

    def get_full_api_url(self) -> str:
        """Get the full API URL with prefix."""
        base = self.endpoint.rstrip("/")
        if self.prefix:
            return f"{base}/{self.prefix}"
        return base


def create_dummy_schema() -> Dict[str, Any]:
    """Create a dummy Iceberg schema with a single string column."""
    return {
        "type": "struct",
        "schema-id": 0,
        "fields": [{"id": 1, "name": "dummy", "required": False, "type": "string"}],
    }


class IcebergNamespace(LanceNamespace):
    """Iceberg REST Catalog namespace implementation for Lance."""

    TABLE_TYPE_LANCE = "lance"
    TABLE_TYPE_KEY = "table_type"

    def __init__(self, **properties):
        """Initialize Iceberg namespace with configuration properties."""
        self.config = IcebergNamespaceConfig(properties)

        headers = {}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"
        if self.config.warehouse:
            headers["X-Iceberg-Access-Delegation"] = "vended-credentials"

        self.rest_client = RestClient(
            base_url=self.config.get_full_api_url(),
            headers=headers,
            connect_timeout=self.config.connect_timeout,
            read_timeout=self.config.read_timeout,
            max_retries=self.config.max_retries,
        )

        logger.info(
            f"Initialized Iceberg namespace with endpoint: {self.config.endpoint}"
        )

    def namespace_id(self) -> str:
        """Return a human-readable unique identifier for this namespace instance."""
        return f"IcebergNamespace {{ endpoint: {self.config.endpoint!r} }}"

    def _encode_namespace(self, namespace: List[str]) -> str:
        """Encode namespace for URL path."""
        encoded_parts = [urllib.parse.quote(s, safe="") for s in namespace]
        joined = NAMESPACE_SEPARATOR.join(encoded_parts)
        return urllib.parse.quote(joined, safe="")

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        """List namespaces."""
        ns_id = self._parse_identifier(request.id)

        try:
            params = {}
            if ns_id:
                parent = self._encode_namespace(ns_id)
                params["parent"] = parent
            if request.page_token:
                params["pageToken"] = request.page_token

            response = self.rest_client.get(
                "/namespaces", params=params if params else None
            )

            namespaces = []
            if response and "namespaces" in response:
                for ns in response["namespaces"]:
                    if ns:
                        namespaces.append(ns[-1])

            namespaces = sorted(set(namespaces))

            return ListNamespacesResponse(namespaces=namespaces)

        except RestClientException as e:
            raise InternalException(f"Failed to list namespaces: {e}")
        except Exception as e:
            raise InternalException(f"Failed to list namespaces: {e}")

    def create_namespace(
        self, request: CreateNamespaceRequest
    ) -> CreateNamespaceResponse:
        """Create a new namespace."""
        ns_id = self._parse_identifier(request.id)

        if not ns_id:
            raise InvalidInputException("Namespace must have at least one level")

        try:
            create_request = {
                "namespace": ns_id,
                "properties": request.properties or {},
            }

            response = self.rest_client.post("/namespaces", create_request)

            logger.info(f"Created namespace: {'.'.join(ns_id)}")

            properties = response.get("properties") if response else {}
            return CreateNamespaceResponse(properties=properties)

        except RestClientException as e:
            if e.is_conflict():
                raise NamespaceAlreadyExistsException(
                    f"Namespace already exists: {'.'.join(request.id)}"
                )
            raise InternalException(f"Failed to create namespace: {e}")
        except (NamespaceAlreadyExistsException, InvalidInputException):
            raise
        except Exception as e:
            raise InternalException(f"Failed to create namespace: {e}")

    def describe_namespace(
        self, request: DescribeNamespaceRequest
    ) -> DescribeNamespaceResponse:
        """Describe a namespace."""
        ns_id = self._parse_identifier(request.id)

        if not ns_id:
            raise InvalidInputException("Namespace must have at least one level")

        try:
            namespace_path = self._encode_namespace(ns_id)
            response = self.rest_client.get(f"/namespaces/{namespace_path}")

            properties = response.get("properties") if response else {}
            return DescribeNamespaceResponse(properties=properties)

        except RestClientException as e:
            if e.is_not_found():
                raise NamespaceNotFoundException(
                    f"Namespace not found: {'.'.join(request.id)}"
                )
            raise InternalException(f"Failed to describe namespace: {e}")
        except (NamespaceNotFoundException, InvalidInputException):
            raise
        except Exception as e:
            raise InternalException(f"Failed to describe namespace: {e}")

    def namespace_exists(self, request: NamespaceExistsRequest) -> None:
        """Check if a namespace exists."""
        describe_request = DescribeNamespaceRequest()
        describe_request.id = request.id
        self.describe_namespace(describe_request)

    def drop_namespace(self, request: DropNamespaceRequest) -> DropNamespaceResponse:
        """Drop a namespace."""
        ns_id = self._parse_identifier(request.id)

        if not ns_id:
            raise InvalidInputException("Namespace must have at least one level")

        try:
            namespace_path = self._encode_namespace(ns_id)
            self.rest_client.delete(f"/namespaces/{namespace_path}")

            logger.info(f"Dropped namespace: {'.'.join(ns_id)}")

            return DropNamespaceResponse(properties={})

        except RestClientException as e:
            if e.is_not_found():
                return DropNamespaceResponse(properties={})
            if e.is_conflict():
                raise InternalException(f"Namespace not empty: {'.'.join(request.id)}")
            raise InternalException(f"Failed to drop namespace: {e}")
        except InvalidInputException:
            raise
        except Exception as e:
            raise InternalException(f"Failed to drop namespace: {e}")

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        """List tables in a namespace."""
        ns_id = self._parse_identifier(request.id)

        if not ns_id:
            raise InvalidInputException("Namespace must have at least one level")

        try:
            namespace_path = self._encode_namespace(ns_id)
            params = {}
            if request.page_token:
                params["pageToken"] = request.page_token

            response = self.rest_client.get(
                f"/namespaces/{namespace_path}/tables",
                params=params if params else None,
            )

            tables = []
            if response and "identifiers" in response:
                for table_id in response["identifiers"]:
                    table_name = table_id.get("name")
                    if table_name and self._is_lance_table(ns_id, table_name):
                        tables.append(table_name)

            tables = sorted(set(tables))

            return ListTablesResponse(tables=tables)

        except RestClientException as e:
            if e.is_not_found():
                raise NamespaceNotFoundException(
                    f"Namespace not found: {'.'.join(ns_id)}"
                )
            raise InternalException(f"Failed to list tables: {e}")
        except (NamespaceNotFoundException, InvalidInputException):
            raise
        except Exception as e:
            raise InternalException(f"Failed to list tables: {e}")

    def create_empty_table(
        self, request: CreateEmptyTableRequest
    ) -> CreateEmptyTableResponse:
        """Create an empty table (metadata only operation)."""
        table_id = self._parse_identifier(request.id)

        if len(table_id) < 2:
            raise InvalidInputException(
                "Table identifier must have at least namespace and table name"
            )

        namespace = table_id[:-1]
        table_name = table_id[-1]

        try:
            table_path = request.location
            if not table_path:
                table_path = f"{self.config.root}/{'/'.join(namespace)}/{table_name}"

            properties = {self.TABLE_TYPE_KEY: self.TABLE_TYPE_LANCE}

            create_request = {
                "name": table_name,
                "location": table_path,
                "schema": create_dummy_schema(),
                "properties": properties,
            }

            namespace_path = self._encode_namespace(namespace)
            self.rest_client.post(
                f"/namespaces/{namespace_path}/tables", create_request
            )

            logger.info(f"Created table: {'.'.join(table_id)}")

            return CreateEmptyTableResponse(location=table_path)

        except RestClientException as e:
            if e.is_conflict():
                raise TableAlreadyExistsException(
                    f"Table already exists: {'.'.join(request.id)}"
                )
            if e.is_not_found():
                raise NamespaceNotFoundException(
                    f"Namespace not found: {'.'.join(namespace)}"
                )
            raise InternalException(f"Failed to create empty table: {e}")
        except (
            TableAlreadyExistsException,
            NamespaceNotFoundException,
            InvalidInputException,
        ):
            raise
        except Exception as e:
            raise InternalException(f"Failed to create empty table: {e}")

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        """Describe a table."""
        table_id = self._parse_identifier(request.id)

        if len(table_id) < 2:
            raise InvalidInputException(
                "Table identifier must have at least namespace and table name"
            )

        namespace = table_id[:-1]
        table_name = table_id[-1]

        try:
            namespace_path = self._encode_namespace(namespace)
            encoded_table_name = urllib.parse.quote(table_name, safe="")

            response = self.rest_client.get(
                f"/namespaces/{namespace_path}/tables/{encoded_table_name}"
            )

            if not response or "metadata" not in response:
                raise TableNotFoundException(f"Table not found: {'.'.join(request.id)}")

            metadata = response["metadata"]
            props = metadata.get("properties", {})

            if (
                not props.get(self.TABLE_TYPE_KEY, "").lower()
                == self.TABLE_TYPE_LANCE.lower()
            ):
                raise InvalidInputException(
                    f"Table {'.'.join(request.id)} is not a Lance table"
                )

            return DescribeTableResponse(
                location=metadata.get("location"), storage_options=props
            )

        except RestClientException as e:
            if e.is_not_found():
                raise TableNotFoundException(f"Table not found: {'.'.join(request.id)}")
            raise InternalException(f"Failed to describe table: {e}")
        except (TableNotFoundException, InvalidInputException):
            raise
        except Exception as e:
            raise InternalException(f"Failed to describe table: {e}")

    def table_exists(self, request: TableExistsRequest) -> None:
        """Check if a table exists."""
        describe_request = DescribeTableRequest()
        describe_request.id = request.id
        self.describe_table(describe_request)

    def deregister_table(
        self, request: DeregisterTableRequest
    ) -> DeregisterTableResponse:
        """Deregister a table (remove from catalog without deleting data)."""
        table_id = self._parse_identifier(request.id)

        if len(table_id) < 2:
            raise InvalidInputException(
                "Table identifier must have at least namespace and table name"
            )

        namespace = table_id[:-1]
        table_name = table_id[-1]

        try:
            namespace_path = self._encode_namespace(namespace)
            encoded_table_name = urllib.parse.quote(table_name, safe="")

            response = self.rest_client.get(
                f"/namespaces/{namespace_path}/tables/{encoded_table_name}"
            )

            table_location = None
            if response and "metadata" in response:
                table_location = response["metadata"].get("location")

            self.rest_client.delete(
                f"/namespaces/{namespace_path}/tables/{encoded_table_name}",
                params={"purgeRequested": "false"},
            )

            logger.info(f"Deregistered table: {'.'.join(table_id)}")

            return DeregisterTableResponse(location=table_location)

        except RestClientException as e:
            if e.is_not_found():
                raise TableNotFoundException(f"Table not found: {'.'.join(request.id)}")
            raise InternalException(f"Failed to deregister table: {e}")
        except (TableNotFoundException, InvalidInputException):
            raise
        except Exception as e:
            raise InternalException(f"Failed to deregister table: {e}")

    def close(self):
        """Close the namespace connection."""
        if self.rest_client:
            self.rest_client.close()

    def _parse_identifier(self, identifier: List[str]) -> List[str]:
        """Parse identifier list."""
        return identifier if identifier else []

    def _is_lance_table(self, namespace: List[str], table_name: str) -> bool:
        """Check if a table is a Lance table."""
        try:
            namespace_path = self._encode_namespace(namespace)
            encoded_table_name = urllib.parse.quote(table_name, safe="")

            response = self.rest_client.get(
                f"/namespaces/{namespace_path}/tables/{encoded_table_name}"
            )

            if response and "metadata" in response:
                props = response["metadata"].get("properties", {})
                return (
                    props.get(self.TABLE_TYPE_KEY, "").lower()
                    == self.TABLE_TYPE_LANCE.lower()
                )
        except Exception as e:
            logger.debug(f"Failed to check if table is Lance table: {e}")
        return False
