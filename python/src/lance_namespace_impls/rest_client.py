"""
Shared REST client infrastructure for Lance namespace implementations.
"""

import json
import logging
from typing import Any, Callable, Dict, Optional, Type, TypeVar

import urllib3
import urllib.parse

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RestClientException(Exception):
    """Exception raised by REST client."""

    def __init__(self, status_code: int, response_body: str):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(f"HTTP {status_code}: {response_body}")

    def is_not_found(self) -> bool:
        """Check if this is a 404 Not Found error."""
        return self.status_code == 404

    def is_conflict(self) -> bool:
        """Check if this is a 409 Conflict error."""
        return self.status_code == 409

    def is_bad_request(self) -> bool:
        """Check if this is a 400 Bad Request error."""
        return self.status_code == 400


class RestClient:
    """
    A reusable REST client for making HTTP requests to REST APIs.

    This client provides:
    - Connection pooling for efficient HTTP connections
    - Configurable timeouts for connect and read operations
    - Retry logic with exponential backoff
    - JSON serialization/deserialization
    - Support for common HTTP methods (GET, POST, DELETE)

    Example usage:
        client = RestClient(
            base_url="http://localhost:8080/api",
            headers={"Authorization": "Bearer token"},
            connect_timeout=10000,
            read_timeout=30000,
            max_retries=3
        )
        response = client.get("/resource")
    """

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        connect_timeout: int = 10000,
        read_timeout: int = 30000,
        max_retries: int = 3,
    ):
        """
        Initialize the REST client.

        Args:
            base_url: The base URL for all requests (e.g., "http://localhost:8080/api")
            headers: Default headers to include with every request
            connect_timeout: Connection timeout in milliseconds (default: 10000)
            read_timeout: Read timeout in milliseconds (default: 30000)
            max_retries: Maximum number of retry attempts (default: 3)
        """
        self.base_url = base_url.rstrip("/")
        self.headers = headers.copy() if headers else {}
        self.headers.setdefault("Content-Type", "application/json")
        self.headers.setdefault("Accept", "application/json")

        timeout = urllib3.Timeout(
            connect=connect_timeout / 1000, read=read_timeout / 1000
        )
        self.http = urllib3.PoolManager(
            timeout=timeout,
            retries=urllib3.Retry(total=max_retries, backoff_factor=0.3),
        )

    def _make_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None,
    ) -> Any:
        """Make HTTP request."""
        url = f"{self.base_url}{path}"

        if params:
            query_string = urllib.parse.urlencode(params)
            url = f"{url}?{query_string}"

        body_data = None
        if body is not None:
            if hasattr(body, "__dict__"):
                body_dict = self._dataclass_to_dict(body)
            elif isinstance(body, dict):
                body_dict = body
            else:
                body_dict = body
            body_data = json.dumps(body_dict).encode("utf-8")

        try:
            response = self.http.request(
                method, url, headers=self.headers, body=body_data
            )

            if response.status >= 400:
                raise RestClientException(response.status, response.data.decode("utf-8"))

            if response.data:
                return json.loads(response.data.decode("utf-8"))
            return None

        except urllib3.exceptions.HTTPError as e:
            raise RestClientException(500, str(e))

    def _dataclass_to_dict(self, obj: Any) -> Dict[str, Any]:
        """Convert dataclass to dictionary, handling nested structures."""
        if hasattr(obj, "__dict__"):
            result = {}
            for key, value in obj.__dict__.items():
                if value is not None:
                    if isinstance(value, list):
                        result[key] = [self._dataclass_to_dict(item) for item in value]
                    elif hasattr(value, "__dict__"):
                        result[key] = self._dataclass_to_dict(value)
                    else:
                        result[key] = value
            return result
        return obj

    def get(
        self,
        path: str,
        params: Optional[Dict[str, str]] = None,
        response_class: Optional[Type[T]] = None,
        response_converter: Optional[Callable[[Dict[str, Any]], T]] = None,
    ) -> Any:
        """
        Make GET request.

        Args:
            path: The URL path (will be appended to base_url)
            params: Optional query parameters
            response_class: Optional class to instantiate with response data
            response_converter: Optional function to convert response dict to object

        Returns:
            The response data (as dict, or converted to response_class/using response_converter)
        """
        response = self._make_request("GET", path, params=params)
        if response_converter and response:
            return response_converter(response)
        if response_class and response:
            return response_class(**response)
        return response

    def post(
        self,
        path: str,
        body: Any,
        response_class: Optional[Type[T]] = None,
        response_converter: Optional[Callable[[Dict[str, Any]], T]] = None,
    ) -> Any:
        """
        Make POST request.

        Args:
            path: The URL path (will be appended to base_url)
            body: The request body (will be JSON serialized)
            response_class: Optional class to instantiate with response data
            response_converter: Optional function to convert response dict to object

        Returns:
            The response data (as dict, or converted to response_class/using response_converter)
        """
        response = self._make_request("POST", path, body=body)
        if response_converter and response:
            return response_converter(response)
        if response_class and response:
            return response_class(**response)
        return response

    def put(
        self,
        path: str,
        body: Any,
        response_class: Optional[Type[T]] = None,
        response_converter: Optional[Callable[[Dict[str, Any]], T]] = None,
    ) -> Any:
        """
        Make PUT request.

        Args:
            path: The URL path (will be appended to base_url)
            body: The request body (will be JSON serialized)
            response_class: Optional class to instantiate with response data
            response_converter: Optional function to convert response dict to object

        Returns:
            The response data (as dict, or converted to response_class/using response_converter)
        """
        response = self._make_request("PUT", path, body=body)
        if response_converter and response:
            return response_converter(response)
        if response_class and response:
            return response_class(**response)
        return response

    def delete(
        self,
        path: str,
        params: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Make DELETE request.

        Args:
            path: The URL path (will be appended to base_url)
            params: Optional query parameters
        """
        self._make_request("DELETE", path, params=params)

    def close(self) -> None:
        """Close the HTTP connection pool."""
        self.http.clear()


class NamespaceException(Exception):
    """Base exception for namespace operations."""

    def __init__(self, message: str):
        super().__init__(message)


class NamespaceNotFoundException(NamespaceException):
    """Exception raised when a namespace is not found."""

    pass


class NamespaceAlreadyExistsException(NamespaceException):
    """Exception raised when a namespace already exists."""

    pass


class TableNotFoundException(NamespaceException):
    """Exception raised when a table is not found."""

    pass


class TableAlreadyExistsException(NamespaceException):
    """Exception raised when a table already exists."""

    pass


class InvalidInputException(NamespaceException):
    """Exception raised for invalid input."""

    pass


class InternalException(NamespaceException):
    """Exception raised for internal errors."""

    pass
