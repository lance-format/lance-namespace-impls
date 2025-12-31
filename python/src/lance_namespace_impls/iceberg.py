"""
Iceberg REST Catalog namespace implementation for Lance.

The prefix (typically a warehouse or catalog name) is treated as part of the
namespace identifier, similar to how Polaris handles catalog names.
For example: [warehouse_name, namespace1, namespace2, ..., table_name]
"""

import logging
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from lance.namespace import LanceNamespace
from lance_namespace_urllib3_client.models import (
    CreateNamespaceRequest,
    CreateNamespaceResponse,
    DeclareTableRequest,
    DeclareTableResponse,
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

NAMESPACE_SEPARATOR = "\x1f"


@dataclass
class IcebergNamespaceConfig:
    """Configuration for Iceberg REST Catalog namespace."""

    ENDPOINT = "endpoint"
    WAREHOUSE = "warehouse"
    AUTH_TOKEN = "auth_token"
    CREDENTIAL = "credential"
    CONNECT_TIMEOUT = "connect_timeout"
    READ_TIMEOUT = "read_timeout"
    MAX_RETRIES = "max_retries"
    ROOT = "root"

    endpoint: str
    warehouse: Optional[str] = None
    auth_token: Optional[str] = None
    credential: Optional[str] = None
    connect_timeout: int = 10000
    read_timeout: int = 30000
    max_retries: int = 3
    root: str = ""

    def __init__(self, properties: Dict[str, str]):
        import os

        self.endpoint = properties.get(self.ENDPOINT)
        if not self.endpoint:
            raise ValueError(f"Required property {self.ENDPOINT} is not set")

        self.warehouse = properties.get(self.WAREHOUSE)
        self.auth_token = properties.get(self.AUTH_TOKEN)
        self.credential = properties.get(self.CREDENTIAL)
        self.connect_timeout = int(properties.get(self.CONNECT_TIMEOUT, "10000"))
        self.read_timeout = int(properties.get(self.READ_TIMEOUT, "30000"))
        self.max_retries = int(properties.get(self.MAX_RETRIES, "3"))
        self.root = properties.get(self.ROOT, os.getcwd())

    def get_base_api_url(self) -> str:
        """Get the base API URL without prefix."""
        return self.endpoint.rstrip("/")


def create_dummy_schema() -> Dict[str, Any]:
    """Create a dummy Iceberg schema with a single string column."""
    return {
        "type": "struct",
        "schema-id": 0,
        "fields": [{"id": 1, "name": "dummy", "required": False, "type": "string"}],
    }


class IcebergNamespace(LanceNamespace):
    """
    Iceberg REST Catalog namespace implementation for Lance.

    The prefix (warehouse) is included in the namespace identifier:
    - Namespace ID format: [prefix, namespace1, namespace2, ...]
    - Table ID format: [prefix, namespace1, namespace2, ..., table_name]

    This is consistent with how Polaris handles catalog names.
    """

    TABLE_TYPE_LANCE = "lance"
    TABLE_TYPE_KEY = "table_type"

    def __init__(self, **properties):
        """Initialize Iceberg namespace with configuration properties."""
        self.config = IcebergNamespaceConfig(properties)
        self._prefix_cache: Dict[str, str] = {}

        headers = {}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"
        if self.config.warehouse:
            headers["X-Iceberg-Access-Delegation"] = "vended-credentials"

        self.rest_client = RestClient(
            base_url=self.config.get_base_api_url(),
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

    def _resolve_prefix(self, warehouse: str) -> str:
        """Resolve warehouse name to actual API prefix.

        Some Iceberg REST catalogs (like Lakekeeper) use a different prefix
        (e.g., warehouse UUID) than the warehouse name. This method calls
        the config endpoint to get the actual prefix.
        """
        if warehouse in self._prefix_cache:
            return self._prefix_cache[warehouse]

        try:
            response = self.rest_client.get(
                "/v1/config", params={"warehouse": warehouse}
            )
            if response and "defaults" in response:
                prefix = response["defaults"].get("prefix")
                if prefix:
                    self._prefix_cache[warehouse] = prefix
                    logger.debug(
                        f"Resolved warehouse '{warehouse}' to prefix '{prefix}'"
                    )
                    return prefix
        except Exception as e:
            logger.debug(f"Failed to resolve prefix for warehouse '{warehouse}': {e}")

        self._prefix_cache[warehouse] = warehouse
        return warehouse

    def _get_prefix_path(self, warehouse: str) -> str:
        """Get the API path with prefix."""
        prefix = self._resolve_prefix(warehouse)
        return f"/v1/{prefix}"

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        """List namespaces.

        The first element of request.id is treated as the prefix (warehouse).
        Remaining elements specify the parent namespace to list children of.
        """
        ns_id = self._parse_identifier(request.id)

        if not ns_id:
            raise InvalidInputException("Must specify at least the prefix (warehouse)")

        try:
            prefix = ns_id[0]
            parent_ns = ns_id[1:] if len(ns_id) > 1 else []
            prefix_path = self._get_prefix_path(prefix)

            params = {}
            if parent_ns:
                parent = self._encode_namespace(parent_ns)
                params["parent"] = parent
            if request.page_token:
                params["pageToken"] = request.page_token

            response = self.rest_client.get(
                f"{prefix_path}/namespaces", params=params if params else None
            )

            namespaces = []
            if response and "namespaces" in response:
                for ns in response["namespaces"]:
                    if ns:
                        full_ns = [prefix] + list(ns)
                        namespaces.append(".".join(full_ns))

            namespaces = sorted(set(namespaces))

            return ListNamespacesResponse(namespaces=namespaces)

        except RestClientException as e:
            raise InternalException(f"Failed to list namespaces: {e}")
        except InvalidInputException:
            raise
        except Exception as e:
            raise InternalException(f"Failed to list namespaces: {e}")

    def create_namespace(
        self, request: CreateNamespaceRequest
    ) -> CreateNamespaceResponse:
        """Create a new namespace.

        The first element of request.id is treated as the prefix (warehouse).
        Remaining elements are the namespace to create.
        """
        ns_id = self._parse_identifier(request.id)

        if len(ns_id) < 2:
            raise InvalidInputException(
                "Namespace must have at least prefix and namespace levels"
            )

        try:
            prefix = ns_id[0]
            namespace = ns_id[1:]
            prefix_path = self._get_prefix_path(prefix)

            create_request = {
                "namespace": namespace,
                "properties": request.properties or {},
            }

            response = self.rest_client.post(
                f"{prefix_path}/namespaces", create_request
            )

            logger.info(f"Created namespace: {prefix}.{'.'.join(namespace)}")

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
        """Describe a namespace.

        The first element of request.id is treated as the prefix (warehouse).
        Remaining elements are the namespace to describe.
        """
        ns_id = self._parse_identifier(request.id)

        if len(ns_id) < 2:
            raise InvalidInputException(
                "Namespace must have at least prefix and namespace levels"
            )

        try:
            prefix = ns_id[0]
            namespace = ns_id[1:]
            prefix_path = self._get_prefix_path(prefix)
            namespace_path = self._encode_namespace(namespace)

            response = self.rest_client.get(
                f"{prefix_path}/namespaces/{namespace_path}"
            )

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

    def drop_namespace(self, request: DropNamespaceRequest) -> DropNamespaceResponse:
        """Drop a namespace.

        The first element of request.id is treated as the prefix (warehouse).
        Remaining elements are the namespace to drop.
        """
        if request.behavior and request.behavior.lower() == "cascade":
            raise InvalidInputException(
                "Cascade behavior is not supported for this implementation"
            )

        ns_id = self._parse_identifier(request.id)

        if len(ns_id) < 2:
            raise InvalidInputException(
                "Namespace must have at least prefix and namespace levels"
            )

        try:
            prefix = ns_id[0]
            namespace = ns_id[1:]
            prefix_path = self._get_prefix_path(prefix)
            namespace_path = self._encode_namespace(namespace)

            self.rest_client.delete(f"{prefix_path}/namespaces/{namespace_path}")

            logger.info(f"Dropped namespace: {prefix}.{'.'.join(namespace)}")

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
        """List tables in a namespace.

        The first element of request.id is treated as the prefix (warehouse).
        Remaining elements are the namespace to list tables from.
        """
        ns_id = self._parse_identifier(request.id)

        if len(ns_id) < 2:
            raise InvalidInputException("Must specify at least prefix and namespace")

        try:
            prefix = ns_id[0]
            namespace = ns_id[1:]
            prefix_path = self._get_prefix_path(prefix)
            namespace_path = self._encode_namespace(namespace)

            params = {}
            if request.page_token:
                params["pageToken"] = request.page_token

            response = self.rest_client.get(
                f"{prefix_path}/namespaces/{namespace_path}/tables",
                params=params if params else None,
            )

            tables = []
            if response and "identifiers" in response:
                for table_id in response["identifiers"]:
                    table_name = table_id.get("name")
                    if table_name and self._is_lance_table(
                        prefix, namespace, table_name
                    ):
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

    def declare_table(self, request: DeclareTableRequest) -> DeclareTableResponse:
        """Declare a table (metadata only operation).

        The first element of request.id is treated as the prefix (warehouse).
        Middle elements are the namespace, last element is the table name.
        """
        table_id = self._parse_identifier(request.id)

        if len(table_id) < 3:
            raise InvalidInputException(
                "Table identifier must have prefix, namespace, and table name"
            )

        prefix = table_id[0]
        namespace = table_id[1:-1]
        table_name = table_id[-1]

        try:
            prefix_path = self._get_prefix_path(prefix)

            table_path = request.location
            if not table_path:
                table_path = (
                    f"{self.config.root}/{'/'.join(table_id[:-1])}/{table_name}"
                )

            properties = {self.TABLE_TYPE_KEY: self.TABLE_TYPE_LANCE}

            create_request = {
                "name": table_name,
                "location": table_path,
                "schema": create_dummy_schema(),
                "properties": properties,
            }

            namespace_path = self._encode_namespace(namespace)
            self.rest_client.post(
                f"{prefix_path}/namespaces/{namespace_path}/tables", create_request
            )

            logger.info(f"Declared table: {'.'.join(table_id)}")

            return DeclareTableResponse(location=table_path)

        except RestClientException as e:
            if e.is_conflict():
                raise TableAlreadyExistsException(
                    f"Table already exists: {'.'.join(request.id)}"
                )
            if e.is_not_found():
                raise NamespaceNotFoundException(
                    f"Namespace not found: {prefix}.{'.'.join(namespace)}"
                )
            raise InternalException(f"Failed to declare table: {e}")
        except (
            TableAlreadyExistsException,
            NamespaceNotFoundException,
            InvalidInputException,
        ):
            raise
        except Exception as e:
            raise InternalException(f"Failed to declare table: {e}")

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        """Describe a table.

        The first element of request.id is treated as the prefix (warehouse).
        Middle elements are the namespace, last element is the table name.
        """
        if request.load_detailed_metadata:
            raise InvalidInputException(
                "load_detailed_metadata=true is not supported for this implementation"
            )

        table_id = self._parse_identifier(request.id)

        if len(table_id) < 3:
            raise InvalidInputException(
                "Table identifier must have prefix, namespace, and table name"
            )

        prefix = table_id[0]
        namespace = table_id[1:-1]
        table_name = table_id[-1]

        try:
            prefix_path = self._get_prefix_path(prefix)
            namespace_path = self._encode_namespace(namespace)
            encoded_table_name = urllib.parse.quote(table_name, safe="")

            response = self.rest_client.get(
                f"{prefix_path}/namespaces/{namespace_path}/tables/{encoded_table_name}"
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

    def deregister_table(
        self, request: DeregisterTableRequest
    ) -> DeregisterTableResponse:
        """Deregister a table (remove from catalog without deleting data).

        The first element of request.id is treated as the prefix (warehouse).
        Middle elements are the namespace, last element is the table name.
        """
        table_id = self._parse_identifier(request.id)

        if len(table_id) < 3:
            raise InvalidInputException(
                "Table identifier must have prefix, namespace, and table name"
            )

        prefix = table_id[0]
        namespace = table_id[1:-1]
        table_name = table_id[-1]

        try:
            prefix_path = self._get_prefix_path(prefix)
            namespace_path = self._encode_namespace(namespace)
            encoded_table_name = urllib.parse.quote(table_name, safe="")

            response = self.rest_client.get(
                f"{prefix_path}/namespaces/{namespace_path}/tables/{encoded_table_name}"
            )

            table_location = None
            if response and "metadata" in response:
                table_location = response["metadata"].get("location")

            self.rest_client.delete(
                f"{prefix_path}/namespaces/{namespace_path}/tables/{encoded_table_name}",
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

    def _is_lance_table(
        self, prefix: str, namespace: List[str], table_name: str
    ) -> bool:
        """Check if a table is a Lance table."""
        try:
            prefix_path = self._get_prefix_path(prefix)
            namespace_path = self._encode_namespace(namespace)
            encoded_table_name = urllib.parse.quote(table_name, safe="")

            response = self.rest_client.get(
                f"{prefix_path}/namespaces/{namespace_path}/tables/{encoded_table_name}"
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
