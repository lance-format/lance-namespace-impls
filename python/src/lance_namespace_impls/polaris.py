"""
Polaris Catalog namespace implementation for Lance.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

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


@dataclass
class PolarisNamespaceConfig:
    """Configuration for Polaris Catalog namespace."""

    ENDPOINT = "polaris.endpoint"
    AUTH_TOKEN = "polaris.auth_token"
    CONNECT_TIMEOUT = "polaris.connect_timeout_millis"
    READ_TIMEOUT = "polaris.read_timeout_millis"
    MAX_RETRIES = "polaris.max_retries"
    ROOT = "polaris.root"

    endpoint: str
    auth_token: Optional[str] = None
    connect_timeout: int = 10000
    read_timeout: int = 30000
    max_retries: int = 3
    root: str = "/tmp/lance"

    def __init__(self, properties: Dict[str, str]):
        self.endpoint = properties.get(self.ENDPOINT)
        if not self.endpoint:
            raise ValueError(f"Required property {self.ENDPOINT} is not set")

        self.auth_token = properties.get(self.AUTH_TOKEN)
        self.connect_timeout = int(properties.get(self.CONNECT_TIMEOUT, "10000"))
        self.read_timeout = int(properties.get(self.READ_TIMEOUT, "30000"))
        self.max_retries = int(properties.get(self.MAX_RETRIES, "3"))
        self.root = properties.get(self.ROOT, "/tmp/lance")

    def get_full_api_url(self) -> str:
        """Get the full API URL for Polaris catalog operations."""
        return self.endpoint.rstrip("/") + "/api/catalog"


class PolarisNamespace(LanceNamespace):
    """Polaris Catalog namespace implementation for Lance."""

    TABLE_FORMAT_LANCE = "lance"
    TABLE_TYPE_KEY = "table_type"

    def __init__(self, **properties):
        """Initialize Polaris namespace with configuration properties."""
        self.config = PolarisNamespaceConfig(properties)

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
            f"Initialized Polaris namespace with endpoint: {self.config.endpoint}"
        )

    def namespace_id(self) -> str:
        """Return a human-readable unique identifier for this namespace instance."""
        return f"PolarisNamespace {{ endpoint: {self.config.endpoint!r} }}"

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        """List namespaces."""
        ns_id = self._parse_identifier(request.id)

        if not ns_id:
            raise InvalidInputException("Must specify at least the catalog")

        try:
            catalog = ns_id[0]
            if len(ns_id) == 1:
                # List namespaces at catalog level
                path = f"/v1/{catalog}/namespaces"
            else:
                # List nested namespaces
                parent_path = ".".join(ns_id[1:])
                path = f"/v1/{catalog}/namespaces/{parent_path}/namespaces"

            response = self.rest_client.get(path)

            namespaces = []
            if response and "namespaces" in response:
                for ns in response["namespaces"]:
                    if ns:
                        # Prefix with catalog name
                        full_ns = [catalog] + list(ns)
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
        """Create a new namespace."""
        ns_id = self._parse_identifier(request.id)

        if len(ns_id) < 2:
            raise InvalidInputException(
                "Namespace must have at least catalog and namespace levels"
            )

        try:
            catalog = ns_id[0]
            namespace = ns_id[1:]

            create_request = {
                "namespace": namespace,
                "properties": request.properties or {},
            }

            response = self.rest_client.post(
                f"/v1/{catalog}/namespaces", create_request
            )

            logger.info(f"Created namespace: {catalog}.{'.'.join(namespace)}")

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

        if len(ns_id) < 2:
            raise InvalidInputException(
                "Namespace must have at least catalog and namespace levels"
            )

        try:
            catalog = ns_id[0]
            namespace_path = ".".join(ns_id[1:])
            response = self.rest_client.get(
                f"/v1/{catalog}/namespaces/{namespace_path}"
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
        """Drop a namespace. Only RESTRICT mode is supported."""
        if request.behavior and request.behavior.lower() == "cascade":
            raise InvalidInputException(
                "Cascade behavior is not supported for this implementation"
            )

        ns_id = self._parse_identifier(request.id)

        if len(ns_id) < 2:
            raise InvalidInputException(
                "Namespace must have at least catalog and namespace levels"
            )

        try:
            catalog = ns_id[0]
            namespace_path = ".".join(ns_id[1:])
            self.rest_client.delete(f"/v1/{catalog}/namespaces/{namespace_path}")

            logger.info(f"Dropped namespace: {catalog}.{namespace_path}")

            return DropNamespaceResponse(properties={})

        except RestClientException as e:
            if e.is_not_found():
                return DropNamespaceResponse(properties={})
            raise InternalException(f"Failed to drop namespace: {e}")
        except InvalidInputException:
            raise
        except Exception as e:
            raise InternalException(f"Failed to drop namespace: {e}")

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        """List tables in a namespace."""
        ns_id = self._parse_identifier(request.id)

        if len(ns_id) < 2:
            raise InvalidInputException("Must specify at least catalog and namespace")

        try:
            catalog = ns_id[0]
            namespace_path = ".".join(ns_id[1:])
            response = self.rest_client.get(
                f"/polaris/v1/{catalog}/namespaces/{namespace_path}/generic-tables"
            )

            tables = []
            if response and "identifiers" in response:
                for table_id in response["identifiers"]:
                    table_name = table_id.get("name")
                    if table_name:
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
        """Declare a table (metadata only operation)."""
        table_id = self._parse_identifier(request.id)

        if len(table_id) < 3:
            raise InvalidInputException(
                "Table identifier must have catalog, namespace, and table name"
            )

        catalog = table_id[0]
        namespace = table_id[1:-1]
        table_name = table_id[-1]

        try:
            table_path = request.location
            if not table_path:
                table_path = (
                    f"{self.config.root}/{'/'.join(table_id[:-1])}/{table_name}"
                )

            properties = {self.TABLE_TYPE_KEY: self.TABLE_FORMAT_LANCE}

            create_request = {
                "name": table_name,
                "format": self.TABLE_FORMAT_LANCE,
                "base-location": table_path,
                "properties": properties,
            }

            namespace_path = ".".join(namespace)
            self.rest_client.post(
                f"/polaris/v1/{catalog}/namespaces/{namespace_path}/generic-tables",
                create_request,
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
                    f"Namespace not found: {catalog}.{'.'.join(namespace)}"
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

        Only load_detailed_metadata=false is supported. Returns location and storage_options only.
        """
        if request.load_detailed_metadata:
            raise InvalidInputException(
                "load_detailed_metadata=true is not supported for this implementation"
            )

        table_id = self._parse_identifier(request.id)

        if len(table_id) < 3:
            raise InvalidInputException(
                "Table identifier must have catalog, namespace, and table name"
            )

        catalog = table_id[0]
        namespace = table_id[1:-1]
        table_name = table_id[-1]

        try:
            namespace_path = ".".join(namespace)

            response = self.rest_client.get(
                f"/polaris/v1/{catalog}/namespaces/{namespace_path}/generic-tables/{table_name}"
            )

            if not response or "table" not in response:
                raise TableNotFoundException(f"Table not found: {'.'.join(request.id)}")

            table = response["table"]
            table_format = table.get("format", "")

            if table_format.lower() != self.TABLE_FORMAT_LANCE:
                raise InvalidInputException(
                    f"Table {'.'.join(request.id)} is not a Lance table (format: {table_format})"
                )

            return DescribeTableResponse(
                location=table.get("base-location"),
                storage_options=table.get("properties", {}),
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
        """Deregister a table (remove from catalog without deleting data)."""
        table_id = self._parse_identifier(request.id)

        if len(table_id) < 3:
            raise InvalidInputException(
                "Table identifier must have catalog, namespace, and table name"
            )

        catalog = table_id[0]
        namespace = table_id[1:-1]
        table_name = table_id[-1]

        try:
            namespace_path = ".".join(namespace)

            response = self.rest_client.get(
                f"/polaris/v1/{catalog}/namespaces/{namespace_path}/generic-tables/{table_name}"
            )

            table_location = None
            if response and "table" in response:
                table_location = response["table"].get("base-location")

            self.rest_client.delete(
                f"/polaris/v1/{catalog}/namespaces/{namespace_path}/generic-tables/{table_name}"
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
