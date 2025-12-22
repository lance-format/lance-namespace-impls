"""
Iceberg REST Catalog namespace implementation for Lance.
"""

import json
import logging
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import urllib3
import urllib.parse

from lance_namespace_urllib3_client.models import (
    ListNamespacesRequest,
    ListNamespacesResponse,
    DescribeNamespaceRequest,
    DescribeNamespaceResponse,
    CreateNamespaceRequest,
    CreateNamespaceResponse,
    DropNamespaceRequest,
    DropNamespaceResponse,
    NamespaceExistsRequest,
    ListTablesRequest,
    ListTablesResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    TableExistsRequest,
    DropTableRequest,
    DropTableResponse,
    CreateEmptyTableRequest,
    CreateEmptyTableResponse,
)

from lance.namespace import LanceNamespace


logger = logging.getLogger(__name__)

NAMESPACE_SEPARATOR = '\x1F'


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
        base = self.endpoint.rstrip('/')
        if self.prefix:
            return f"{base}/{self.prefix}"
        return base


class RestClient:
    """Simple REST client for Iceberg REST Catalog API."""

    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None,
                 connect_timeout: int = 10, read_timeout: int = 30, max_retries: int = 3):
        self.base_url = base_url.rstrip('/')
        self.headers = headers or {}
        self.headers['Content-Type'] = 'application/json'
        self.headers['Accept'] = 'application/json'

        timeout = urllib3.Timeout(connect=connect_timeout/1000, read=read_timeout/1000)
        self.http = urllib3.PoolManager(
            timeout=timeout,
            retries=urllib3.Retry(total=max_retries, backoff_factor=0.3)
        )

    def _make_request(self, method: str, path: str, params: Optional[Dict[str, str]] = None,
                      body: Optional[Any] = None) -> Any:
        """Make HTTP request to Iceberg API."""
        url = f"{self.base_url}{path}"

        if params:
            query_string = urllib.parse.urlencode(params)
            url = f"{url}?{query_string}"

        body_data = None
        if body is not None:
            body_data = json.dumps(body).encode('utf-8')

        try:
            response = self.http.request(
                method,
                url,
                headers=self.headers,
                body=body_data
            )

            if response.status >= 400:
                raise RestClientException(response.status, response.data.decode('utf-8'))

            if response.data:
                return json.loads(response.data.decode('utf-8'))
            return None

        except urllib3.exceptions.HTTPError as e:
            raise RestClientException(500, str(e))

    def get(self, path: str, params: Optional[Dict[str, str]] = None) -> Any:
        """Make GET request."""
        return self._make_request('GET', path, params=params)

    def post(self, path: str, body: Any) -> Any:
        """Make POST request."""
        return self._make_request('POST', path, body=body)

    def delete(self, path: str, params: Optional[Dict[str, str]] = None) -> None:
        """Make DELETE request."""
        self._make_request('DELETE', path, params=params)

    def close(self):
        """Close the HTTP connection pool."""
        self.http.clear()


class RestClientException(Exception):
    """Exception raised by REST client."""

    def __init__(self, status_code: int, response_body: str):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(f"HTTP {status_code}: {response_body}")


class LanceNamespaceException(Exception):
    """Exception for Lance namespace operations."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)

    @classmethod
    def not_found(cls, message: str, error_code: str, resource: str, details: str = ""):
        """Create a not found exception."""
        full_message = f"{message} [{error_code}]: {resource}"
        if details:
            full_message += f" - {details}"
        return cls(404, full_message)

    @classmethod
    def bad_request(cls, message: str, error_code: str, resource: str, details: str = ""):
        """Create a bad request exception."""
        full_message = f"{message} [{error_code}]: {resource}"
        if details:
            full_message += f" - {details}"
        return cls(400, full_message)

    @classmethod
    def conflict(cls, message: str, error_code: str, resource: str, details: str = ""):
        """Create a conflict exception."""
        full_message = f"{message} [{error_code}]: {resource}"
        if details:
            full_message += f" - {details}"
        return cls(409, full_message)


def create_dummy_schema() -> Dict[str, Any]:
    """Create a dummy Iceberg schema with a single string column."""
    return {
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {
                "id": 1,
                "name": "dummy",
                "required": False,
                "type": "string"
            }
        ]
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
            headers['Authorization'] = f"Bearer {self.config.auth_token}"
        if self.config.warehouse:
            headers['X-Iceberg-Access-Delegation'] = 'vended-credentials'

        self.rest_client = RestClient(
            base_url=self.config.get_full_api_url(),
            headers=headers,
            connect_timeout=self.config.connect_timeout,
            read_timeout=self.config.read_timeout,
            max_retries=self.config.max_retries
        )

        logger.info(f"Initialized Iceberg namespace with endpoint: {self.config.endpoint}")

    def namespace_id(self) -> str:
        """Return a human-readable unique identifier for this namespace instance."""
        return f"IcebergNamespace {{ endpoint: {self.config.endpoint!r} }}"

    def _encode_namespace(self, namespace: List[str]) -> str:
        """Encode namespace for URL path."""
        encoded_parts = [urllib.parse.quote(s, safe='') for s in namespace]
        joined = NAMESPACE_SEPARATOR.join(encoded_parts)
        return urllib.parse.quote(joined, safe='')

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        """List namespaces."""
        ns_id = self._parse_identifier(request.id)

        try:
            params = {}
            if ns_id:
                parent = self._encode_namespace(ns_id)
                params['parent'] = parent
            if request.page_token:
                params['pageToken'] = request.page_token

            response = self.rest_client.get('/namespaces', params=params if params else None)

            namespaces = []
            if response and 'namespaces' in response:
                for ns in response['namespaces']:
                    if ns:
                        namespaces.append(ns[-1])

            namespaces = sorted(set(namespaces))

            result = ListNamespacesResponse()
            result.namespaces = namespaces
            return result

        except Exception as e:
            if isinstance(e, LanceNamespaceException):
                raise
            raise LanceNamespaceException(500, f"Failed to list namespaces: {e}")

    def create_namespace(self, request: CreateNamespaceRequest) -> CreateNamespaceResponse:
        """Create a new namespace."""
        ns_id = self._parse_identifier(request.id)

        if not ns_id:
            raise ValueError("Namespace must have at least one level")

        try:
            create_request = {
                "namespace": ns_id,
                "properties": request.properties or {}
            }

            response = self.rest_client.post('/namespaces', create_request)

            result = CreateNamespaceResponse()
            result.properties = response.get('properties') if response else None
            return result

        except RestClientException as e:
            if e.status_code == 409:
                raise LanceNamespaceException.conflict(
                    "Namespace already exists",
                    "NAMESPACE_EXISTS",
                    '.'.join(request.id),
                    e.response_body
                )
            raise LanceNamespaceException(500, f"Failed to create namespace: {e}")
        except Exception as e:
            raise LanceNamespaceException(500, f"Failed to create namespace: {e}")

    def describe_namespace(self, request: DescribeNamespaceRequest) -> DescribeNamespaceResponse:
        """Describe a namespace."""
        ns_id = self._parse_identifier(request.id)

        if not ns_id:
            raise ValueError("Namespace must have at least one level")

        try:
            namespace_path = self._encode_namespace(ns_id)
            response = self.rest_client.get(f"/namespaces/{namespace_path}")

            result = DescribeNamespaceResponse()
            result.properties = response.get('properties') if response else None
            return result

        except RestClientException as e:
            if e.status_code == 404:
                raise LanceNamespaceException.not_found(
                    "Namespace not found",
                    "NAMESPACE_NOT_FOUND",
                    '.'.join(request.id),
                    e.response_body
                )
            raise LanceNamespaceException(500, f"Failed to describe namespace: {e}")
        except Exception as e:
            raise LanceNamespaceException(500, f"Failed to describe namespace: {e}")

    def namespace_exists(self, request: NamespaceExistsRequest) -> None:
        """Check if a namespace exists."""
        describe_request = DescribeNamespaceRequest()
        describe_request.id = request.id
        self.describe_namespace(describe_request)

    def drop_namespace(self, request: DropNamespaceRequest) -> DropNamespaceResponse:
        """Drop a namespace."""
        ns_id = self._parse_identifier(request.id)

        if not ns_id:
            raise ValueError("Namespace must have at least one level")

        try:
            namespace_path = self._encode_namespace(ns_id)
            self.rest_client.delete(f"/namespaces/{namespace_path}")

            return DropNamespaceResponse()

        except RestClientException as e:
            if e.status_code == 404:
                return DropNamespaceResponse()
            if e.status_code == 409:
                raise LanceNamespaceException.conflict(
                    "Namespace not empty",
                    "NAMESPACE_NOT_EMPTY",
                    '.'.join(request.id),
                    e.response_body
                )
            raise LanceNamespaceException(500, f"Failed to drop namespace: {e}")
        except Exception as e:
            raise LanceNamespaceException(500, f"Failed to drop namespace: {e}")

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        """List tables in a namespace."""
        ns_id = self._parse_identifier(request.id)

        if not ns_id:
            raise ValueError("Namespace must have at least one level")

        try:
            namespace_path = self._encode_namespace(ns_id)
            params = {}
            if request.page_token:
                params['pageToken'] = request.page_token

            response = self.rest_client.get(
                f"/namespaces/{namespace_path}/tables",
                params=params if params else None
            )

            tables = []
            if response and 'identifiers' in response:
                for table_id in response['identifiers']:
                    table_name = table_id.get('name')
                    if table_name and self._is_lance_table(ns_id, table_name):
                        tables.append(table_name)

            tables = sorted(set(tables))

            result = ListTablesResponse()
            result.tables = tables
            return result

        except Exception as e:
            if isinstance(e, LanceNamespaceException):
                raise
            raise LanceNamespaceException(500, f"Failed to list tables: {e}")

    def create_empty_table(self, request: CreateEmptyTableRequest) -> CreateEmptyTableResponse:
        """Create an empty table (metadata only operation)."""
        table_id = self._parse_identifier(request.id)

        if len(table_id) < 2:
            raise ValueError("Table identifier must have at least namespace and table name")

        namespace = table_id[:-1]
        table_name = table_id[-1]

        try:
            table_path = request.location
            if not table_path:
                table_path = f"{self.config.root}/{'/'.join(namespace)}/{table_name}"

            properties = {
                self.TABLE_TYPE_KEY: self.TABLE_TYPE_LANCE
            }
            if request.properties:
                properties.update(request.properties)

            create_request = {
                "name": table_name,
                "location": table_path,
                "schema": create_dummy_schema(),
                "properties": properties
            }

            namespace_path = self._encode_namespace(namespace)
            response = self.rest_client.post(
                f"/namespaces/{namespace_path}/tables",
                create_request
            )

            result = CreateEmptyTableResponse()
            result.location = table_path
            if response and 'metadata' in response:
                result.properties = response['metadata'].get('properties')
            return result

        except RestClientException as e:
            if e.status_code == 409:
                raise LanceNamespaceException.conflict(
                    "Table already exists",
                    "TABLE_EXISTS",
                    '.'.join(request.id),
                    e.response_body
                )
            if e.status_code == 404:
                raise LanceNamespaceException.not_found(
                    "Namespace not found",
                    "NAMESPACE_NOT_FOUND",
                    '.'.join(namespace),
                    e.response_body
                )
            raise LanceNamespaceException(500, f"Failed to create empty table: {e}")
        except Exception as e:
            raise LanceNamespaceException(500, f"Failed to create empty table: {e}")

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        """Describe a table."""
        table_id = self._parse_identifier(request.id)

        if len(table_id) < 2:
            raise ValueError("Table identifier must have at least namespace and table name")

        namespace = table_id[:-1]
        table_name = table_id[-1]

        try:
            namespace_path = self._encode_namespace(namespace)
            encoded_table_name = urllib.parse.quote(table_name, safe='')

            response = self.rest_client.get(
                f"/namespaces/{namespace_path}/tables/{encoded_table_name}"
            )

            if not response or 'metadata' not in response:
                raise LanceNamespaceException.not_found(
                    "Table not found",
                    "TABLE_NOT_FOUND",
                    '.'.join(request.id),
                    "No metadata"
                )

            metadata = response['metadata']
            props = metadata.get('properties', {})

            if not props.get(self.TABLE_TYPE_KEY, '').lower() == self.TABLE_TYPE_LANCE.lower():
                raise LanceNamespaceException.bad_request(
                    "Not a Lance table",
                    "INVALID_TABLE",
                    '.'.join(request.id),
                    "Table is not managed by Lance"
                )

            result = DescribeTableResponse()
            result.location = metadata.get('location')
            result.properties = props
            return result

        except RestClientException as e:
            if e.status_code == 404:
                raise LanceNamespaceException.not_found(
                    "Table not found",
                    "TABLE_NOT_FOUND",
                    '.'.join(request.id),
                    e.response_body
                )
            raise LanceNamespaceException(500, f"Failed to describe table: {e}")
        except Exception as e:
            if isinstance(e, LanceNamespaceException):
                raise
            raise LanceNamespaceException(500, f"Failed to describe table: {e}")

    def table_exists(self, request: TableExistsRequest) -> None:
        """Check if a table exists."""
        describe_request = DescribeTableRequest()
        describe_request.id = request.id
        self.describe_table(describe_request)

    def drop_table(self, request: DropTableRequest) -> DropTableResponse:
        """Drop a table."""
        table_id = self._parse_identifier(request.id)

        if len(table_id) < 2:
            raise ValueError("Table identifier must have at least namespace and table name")

        namespace = table_id[:-1]
        table_name = table_id[-1]

        try:
            namespace_path = self._encode_namespace(namespace)
            encoded_table_name = urllib.parse.quote(table_name, safe='')

            table_location = None
            try:
                response = self.rest_client.get(
                    f"/namespaces/{namespace_path}/tables/{encoded_table_name}"
                )
                if response and 'metadata' in response:
                    table_location = response['metadata'].get('location')
            except RestClientException as e:
                if e.status_code == 404:
                    result = DropTableResponse()
                    result.id = request.id
                    return result

            self.rest_client.delete(
                f"/namespaces/{namespace_path}/tables/{encoded_table_name}",
                params={'purgeRequested': 'false'}
            )

            result = DropTableResponse()
            result.id = request.id
            result.location = table_location
            return result

        except Exception as e:
            if isinstance(e, LanceNamespaceException):
                raise
            raise LanceNamespaceException(500, f"Failed to drop table: {e}")

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
            encoded_table_name = urllib.parse.quote(table_name, safe='')

            response = self.rest_client.get(
                f"/namespaces/{namespace_path}/tables/{encoded_table_name}"
            )

            if response and 'metadata' in response:
                props = response['metadata'].get('properties', {})
                return props.get(self.TABLE_TYPE_KEY, '').lower() == self.TABLE_TYPE_LANCE.lower()
        except Exception as e:
            logger.debug(f"Failed to check if table is Lance table: {e}")
        return False
