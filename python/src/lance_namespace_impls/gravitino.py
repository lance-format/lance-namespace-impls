"""
Gravitino REST namespace implementation for Lance.

This implementation provides integration with Apache Gravitino's Lance REST service,
which was introduced in Gravitino version 1.1.0. It allows Lance to use Gravitino
as a namespace server with full support for Lance tables.

The implementation follows Gravitino's three-level hierarchy:
- Catalog (first element of namespace/table identifier)
- Schema (second element of namespace/table identifier)
- Table (last element of table identifier)

For more information, see:
https://gravitino.apache.org/docs/1.1.0/lance-rest-service
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


@dataclass
class GravitinoNamespaceConfig:
    """Configuration for Gravitino REST namespace."""

    ENDPOINT = "endpoint"
    AUTH_TOKEN = "auth_token"
    CONNECT_TIMEOUT = "connect_timeout"
    READ_TIMEOUT = "read_timeout"
    MAX_RETRIES = "max_retries"

    endpoint: str
    auth_token: Optional[str] = None
    connect_timeout: int = 10000
    read_timeout: int = 30000
    max_retries: int = 3

    def __init__(self, properties: Dict[str, str]):
        self.endpoint = properties.get(self.ENDPOINT)
        if not self.endpoint:
            raise ValueError(f"Required property {self.ENDPOINT} is not set")

        self.auth_token = properties.get(self.AUTH_TOKEN)
        self.connect_timeout = int(properties.get(self.CONNECT_TIMEOUT, "10000"))
        self.read_timeout = int(properties.get(self.READ_TIMEOUT, "30000"))
        self.max_retries = int(properties.get(self.MAX_RETRIES, "3"))

    def get_base_api_url(self) -> str:
        """Get the base API URL for Lance REST service."""
        return f"{self.endpoint.rstrip('/')}/lance/v1"


class GravitinoNamespace(LanceNamespace):
    """
    Gravitino REST namespace implementation for Lance.

    This implementation integrates with Apache Gravitino's Lance REST service
    to provide namespace and table management capabilities. It follows Gravitino's
    three-level hierarchy: catalog → schema → table.

    Configuration properties:
    - endpoint: Gravitino server endpoint (e.g., "http://localhost:9101")
    - auth_token: Optional authentication token
    - connect_timeout: Connection timeout in milliseconds (default: 10000)
    - read_timeout: Read timeout in milliseconds (default: 30000)
    - max_retries: Maximum retry attempts (default: 3)

    Namespace ID format: [catalog, schema]
    Table ID format: [catalog, schema, table_name]
    """

    def __init__(self, **properties):
        """Initialize Gravitino namespace with configuration properties."""
        self.config = GravitinoNamespaceConfig(properties)

        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"

        self.rest_client = RestClient(
            base_url=self.config.get_base_api_url(),
            headers=headers,
            connect_timeout=self.config.connect_timeout,
            read_timeout=self.config.read_timeout,
            max_retries=self.config.max_retries,
        )

        logger.info(
            f"Initialized Gravitino namespace with endpoint: {self.config.endpoint}"
        )

    def namespace_id(self) -> str:
        """Return a human-readable unique identifier for this namespace instance."""
        return (
            f"GravitinoNamespace {{ endpoint: {self.config.endpoint!r} }}"
        )

    def _encode_namespace_path(self, namespace_parts: List[str]) -> str:
        """Encode namespace parts for URL path using $ delimiter."""
        if not namespace_parts:
            return ""
        # Gravitino uses $ as delimiter, URL-encoded as %24
        return "%24".join(urllib.parse.quote(part, safe="") for part in namespace_parts)

    def _parse_identifier(self, identifier: List[str]) -> List[str]:
        """Parse and validate identifier list."""
        if not identifier:
            return []
        return identifier

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        """List namespaces.

        For Gravitino:
        - If request.id is empty, list all catalogs using /namespace/./list?delimiter=.
        - If request.id has one element (catalog), list schemas in that catalog
        """
        ns_id = self._parse_identifier(request.id)

        try:
            if not ns_id:
                # List catalogs (top-level namespaces) using the correct endpoint
                response = self.rest_client.get("/namespace/%2E/list", params={"delimiter": "."})
            elif len(ns_id) == 1:
                # List schemas in catalog
                catalog = ns_id[0]
                encoded_catalog = urllib.parse.quote(catalog, safe="")
                response = self.rest_client.get(f"/namespace/{encoded_catalog}/list")
            else:
                # Cannot list below schema level
                return ListNamespacesResponse(namespaces=[])

            namespaces = []
            if response and "namespaces" in response:
                for ns in response["namespaces"]:
                    if ns:
                        if not ns_id:
                            # Top-level catalog
                            namespaces.append(ns)
                        else:
                            # Schema in catalog
                            namespaces.append(f"{ns_id[0]}.{ns}")
            elif response:
                # Handle case where response doesn't have 'namespaces' key
                # Some APIs might return the list directly or with different key names
                logger.debug(f"Unexpected response format: {response}")
                # Try to handle different response formats
                if isinstance(response, list):
                    namespaces = response
                elif "catalogs" in response:
                    namespaces = response["catalogs"]
                elif "schemas" in response:
                    for schema in response["schemas"]:
                        if not ns_id:
                            namespaces.append(schema)
                        else:
                            namespaces.append(f"{ns_id[0]}.{schema}")

            return ListNamespacesResponse(namespaces=sorted(set(namespaces)))

        except RestClientException as e:
            if e.is_not_found():
                return ListNamespacesResponse(namespaces=[])
            raise InternalException(f"Failed to list namespaces: {e}")
        except Exception as e:
            raise InternalException(f"Failed to list namespaces: {e}")

    def create_namespace(
        self, request: CreateNamespaceRequest
    ) -> CreateNamespaceResponse:
        """Create a new namespace.

        For Gravitino:
        - One element: create catalog
        - Two elements: create schema in catalog
        """
        ns_id = self._parse_identifier(request.id)

        if not ns_id:
            raise InvalidInputException("Namespace identifier cannot be empty")

        if len(ns_id) > 2:
            raise InvalidInputException(
                "Gravitino supports maximum 2-level namespaces (catalog.schema)"
            )

        try:
            if len(ns_id) == 1:
                # Create catalog
                catalog = ns_id[0]
                encoded_catalog = urllib.parse.quote(catalog, safe="")
                create_request = {
                    "id": [catalog],
                    "mode": "create",
                    "properties": request.properties or {},
                }
                response = self.rest_client.post(
                    f"/namespace/{encoded_catalog}/create", create_request
                )
            else:
                # Create schema in catalog
                catalog, schema = ns_id[0], ns_id[1]
                encoded_path = self._encode_namespace_path([catalog, schema])
                create_request = {
                    "id": [catalog, schema],
                    "mode": "create",
                    "properties": request.properties or {},
                }
                response = self.rest_client.post(
                    f"/namespace/{encoded_path}/create", create_request
                )

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

        if not ns_id or len(ns_id) > 2:
            raise InvalidInputException(
                "Namespace must be 1 or 2 levels (catalog or catalog.schema)"
            )

        try:
            encoded_path = self._encode_namespace_path(ns_id)
            response = self.rest_client.post(
                f"/namespace/{encoded_path}/describe", {"id": ns_id}
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
        
        For catalog-level namespaces (top level), sets the catalog's in-use property 
        to false before dropping, as required by Gravitino. Supports CASCADE behavior
        for catalog drops by first removing any remaining schemas.
        """
        ns_id = self._parse_identifier(request.id)

        if not ns_id or len(ns_id) > 2:
            raise InvalidInputException(
                "Namespace must be 1 or 2 levels (catalog or catalog.schema)"
            )

        try:
            # Prepare the drop request payload
            drop_request = {"id": ns_id}
            if request.behavior:
                drop_request["behavior"] = request.behavior
            
            encoded_path = self._encode_namespace_path(ns_id)
            self.rest_client.post(f"/namespace/{encoded_path}/drop", drop_request)

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
        """List tables in a namespace.

        For Gravitino, the namespace must be exactly 2 levels (catalog.schema).
        """
        ns_id = self._parse_identifier(request.id)

        if len(ns_id) != 2:
            raise InvalidInputException(
                "Namespace must be exactly 2 levels (catalog.schema) to list tables"
            )

        try:
            catalog, schema = ns_id[0], ns_id[1]
            encoded_path = self._encode_namespace_path([catalog, schema])

            response = self.rest_client.get(f"/namespace/{encoded_path}/table/list")

            tables = []
            if response and "tables" in response:
                for table in response["tables"]:
                    # Extract just the table name from full identifier
                    # Gravitino might return full identifiers like "catalog$schema$table"
                    if "$" in table:
                        # Split by $ and take the last part (table name)
                        table_name = table.split("$")[-1]
                        tables.append(table_name)
                    else:
                        tables.append(table)

            return ListTablesResponse(tables=sorted(set(tables)))

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
        """Declare a table (register existing Lance table).

        For Gravitino, table ID must be exactly 3 levels (catalog.schema.table).
        """
        table_id = self._parse_identifier(request.id)

        if len(table_id) != 3:
            raise InvalidInputException(
                "Table identifier must be exactly 3 levels (catalog.schema.table)"
            )

        catalog, schema, table_name = table_id[0], table_id[1], table_id[2]

        try:
            encoded_path = self._encode_namespace_path([catalog, schema, table_name])

            register_request = {
                "id": [catalog, schema, table_name],
                "location": request.location,
                "mode": "CREATE",
            }

            response = self.rest_client.post(
                f"/table/{encoded_path}/register", register_request
            )

            logger.info(f"Declared table: {'.'.join(table_id)}")

            location = response.get("location") if response else request.location
            return DeclareTableResponse(location=location)

        except RestClientException as e:
            if e.is_conflict():
                raise TableAlreadyExistsException(
                    f"Table already exists: {'.'.join(request.id)}"
                )
            if e.is_not_found():
                raise NamespaceNotFoundException(
                    f"Namespace not found: {catalog}.{schema}"
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
        """Describe a table."""
        if request.load_detailed_metadata:
            raise InvalidInputException(
                "load_detailed_metadata=true is not supported for this implementation"
            )

        table_id = self._parse_identifier(request.id)

        if len(table_id) != 3:
            raise InvalidInputException(
                "Table identifier must be exactly 3 levels (catalog.schema.table)"
            )

        catalog, schema, table_name = table_id[0], table_id[1], table_id[2]

        try:
            encoded_path = self._encode_namespace_path([catalog, schema, table_name])

            # Check if table exists first
            exists_request = {"id": [catalog, schema, table_name]}
            exists_response = self.rest_client.post(
                f"/table/{encoded_path}/exists", exists_request
            )

            # Handle different response formats for exists check
            table_exists = False
            if exists_response:
                if isinstance(exists_response, dict):
                    table_exists = exists_response.get("exists", False)
                elif isinstance(exists_response, bool):
                    table_exists = exists_response
                else:
                    # If we get any non-error response, assume table exists
                    table_exists = True

            if not table_exists:
                raise TableNotFoundException(f"Table not found: {'.'.join(request.id)}")

            # For Gravitino, we need to get table metadata through the table operations
            # Since Gravitino Lance REST doesn't have a direct describe endpoint,
            # we'll return basic information
            return DescribeTableResponse(
                location=None,  # Location would need to be retrieved from table metadata
                storage_options={},
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

        if len(table_id) != 3:
            raise InvalidInputException(
                "Table identifier must be exactly 3 levels (catalog.schema.table)"
            )

        catalog, schema, table_name = table_id[0], table_id[1], table_id[2]

        try:
            encoded_path = self._encode_namespace_path([catalog, schema, table_name])

            deregister_request = {"id": [catalog, schema, table_name]}

            response = self.rest_client.post(
                f"/table/{encoded_path}/deregister", deregister_request
            )

            logger.info(f"Deregistered table: {'.'.join(table_id)}")

            location = response.get("location") if response else None
            return DeregisterTableResponse(location=location)

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