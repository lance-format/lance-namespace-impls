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
"""

from io import BytesIO
import struct
from typing import List, Optional
from urllib.parse import urlparse
import os
import logging

try:
    from hive_metastore_client import HiveMetastoreClient as Client
    from thrift.protocol import TBinaryProtocol
    from thrift.transport import TSocket
    from thrift.transport.TTransport import (
        CReadableTransport,
        TTransportBase,
        TTransportException,
    )
    from thrift_files.libraries.thrift_hive_metastore_client.ThriftHiveMetastore import (
        Client as ThriftClient,
    )
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
    TBinaryProtocol = None
    TSocket = None

    class CReadableTransport:
        pass

    class TTransportBase:
        pass

    class TTransportException(Exception):
        """Fallback thrift transport exception used when thrift is unavailable."""

        UNKNOWN = 0
        NOT_OPEN = 1
        ALREADY_OPEN = 2
        TIMED_OUT = 3
        END_OF_FILE = 4

        def __init__(self, type=UNKNOWN, message=None):
            self.type = type
            self.message = message
            super().__init__(message)

    class ThriftClient:
        def __init__(self, *args, **kwargs):
            pass

    HiveDatabase = None
    HiveTable = None
    StorageDescriptor = None
    SerDeInfo = None
    FieldSchema = None
    NoSuchObjectException = None
    AlreadyExistsException = None
    InvalidOperationException = None
    MetaException = None

try:
    from puresasl.client import SASLClient
    import puresasl.mechanisms as sasl_mechanisms

    SASL_SUPPORT_AVAILABLE = True
except ImportError:
    SASLClient = None
    sasl_mechanisms = None
    SASL_SUPPORT_AVAILABLE = False

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
    DropTableRequest,
    DropTableResponse,
    ListTablesRequest,
    ListTablesResponse,
    DeclareTableRequest,
    DeclareTableResponse,
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
HIVE_METASTORE_SASL_ENABLED_KEY = "hive.metastore.sasl.enabled"
_SASL_NEGOTIATION_HEADER = struct.Struct(">BI")
_SASL_FRAME_HEADER = struct.Struct(">I")


def _as_bool(value) -> bool:
    """Parse a configuration value into a boolean."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _get_kerberos_service_name(properties: dict) -> str:
    """Resolve the Kerberos service name used for SASL negotiation."""
    explicit_service_name = properties.get("kerberos.service-name")
    if explicit_service_name:
        return explicit_service_name

    metastore_principal = properties.get("hive.metastore.kerberos.principal")
    if metastore_principal and "/" in metastore_principal:
        return metastore_principal.split("/", 1)[0]

    return "hive"


class _KerberosSaslTransport(TTransportBase, CReadableTransport):
    """Minimal SASL transport for Kerberos-authenticated HMS connections."""

    START = 1
    OK = 2
    BAD = 3
    ERROR = 4
    COMPLETE = 5

    def __init__(
        self,
        transport,
        host: str,
        service: str,
        principal: Optional[str] = None,
    ):
        self._transport = transport
        self._host = host
        self._service = service
        self._principal = principal
        self._sasl = None
        self._encoding_enabled = False
        self._wbuf = BytesIO()
        self._rbuf = BytesIO(b"")

    def isOpen(self):
        return self._transport.isOpen()

    def is_open(self):
        return self.isOpen()

    def _create_sasl_client(self):
        if not SASL_SUPPORT_AVAILABLE or not getattr(
            sasl_mechanisms, "have_kerberos", False
        ):
            raise ImportError(
                "Kerberos SASL support requires the 'pure-sasl' and 'kerberos' "
                "packages. Please install with: "
                "pip install 'lance-namespace-impls[hive3]'"
            )

        return SASLClient(
            self._host,
            service=self._service,
            mechanism="GSSAPI",
            principal=self._principal,
        )

    def open(self):
        if not self.isOpen():
            self._transport.open()

        if self._sasl is not None:
            raise TTransportException(
                type=TTransportException.ALREADY_OPEN,
                message="SASL transport is already open",
            )

        self._sasl = self._create_sasl_client()

        try:
            self._send_sasl_message(self.START, self._sasl.mechanism.encode("ascii"))
            initial_response = self._sasl.process()
            self._send_sasl_message(self.OK, initial_response or b"")

            while True:
                status, payload = self._recv_sasl_message()
                if status in (self.BAD, self.ERROR):
                    error = payload.decode("utf-8", errors="replace") or str(status)
                    raise TTransportException(
                        type=TTransportException.NOT_OPEN,
                        message=f"SASL negotiation failed: {error}",
                    )

                if status not in (self.OK, self.COMPLETE):
                    raise TTransportException(
                        type=TTransportException.NOT_OPEN,
                        message=f"Unexpected SASL negotiation status: {status}",
                    )

                if status == self.COMPLETE:
                    # Hive Metastore finishes the SASL negotiation here. In
                    # practice, feeding the COMPLETE payload back into the
                    # Python GSSAPI client causes an invalid-token failure
                    # against real Kerberized HMS deployments.
                    break

                response = self._sasl.process(payload or b"")
                self._send_sasl_message(self.OK, response or b"")
            self._encoding_enabled = self._requires_sasl_encoding()
        except Exception:
            self.close()
            raise

    def close(self):
        try:
            if self._sasl is not None:
                try:
                    self._sasl.dispose()
                except Exception:
                    pass
        finally:
            self._sasl = None
            self._encoding_enabled = False
            self._wbuf = BytesIO()
            self._rbuf = BytesIO(b"")
            self._transport.close()

    def read(self, sz):
        result = self._rbuf.read(sz)
        if len(result) == sz:
            return result

        self._read_frame()
        return result + self._rbuf.read(sz - len(result))

    def write(self, buf):
        self._wbuf.write(buf)

    def flush(self):
        payload = self._wbuf.getvalue()
        self._wbuf = BytesIO()

        if not payload:
            self._transport.flush()
            return

        self._transport.write(self._encode_frame(payload))
        self._transport.flush()

    def _send_sasl_message(self, status: int, body: bytes):
        payload = body or b""
        header = _SASL_NEGOTIATION_HEADER.pack(status, len(payload))
        self._transport.write(header + payload)
        self._transport.flush()

    def _recv_sasl_message(self):
        header = self._read_all_from_transport(5)
        status, length = _SASL_NEGOTIATION_HEADER.unpack(header)
        payload = self._read_all_from_transport(length) if length else b""
        return status, payload

    def _encode_frame(self, payload: bytes) -> bytes:
        encoded = self._sasl.wrap(payload) if self._encoding_enabled else payload
        return _SASL_FRAME_HEADER.pack(len(encoded)) + encoded

    def _read_frame(self):
        header = self._read_all_from_transport(4)
        (length,) = _SASL_FRAME_HEADER.unpack(header)
        payload = self._read_all_from_transport(length) if length else b""
        self._rbuf = BytesIO(self._decode_frame(payload))

    def _decode_frame(self, payload: bytes) -> bytes:
        if self._encoding_enabled:
            return self._sasl.unwrap(payload)
        return payload

    def _requires_sasl_encoding(self) -> bool:
        qop = getattr(self._sasl, "qop", "auth")
        if isinstance(qop, bytes):
            qop = qop.decode("ascii", errors="ignore")
        return str(qop).lower() not in {"", "auth"}

    def _read_all_from_transport(self, size: int) -> bytes:
        try:
            return self._transport.readAll(size)
        except EOFError as exc:
            raise TTransportException(
                type=TTransportException.END_OF_FILE,
                message="End of file reading from transport",
            ) from exc

    @property
    def cstringio_buf(self):
        return self._rbuf

    def cstringio_refill(self, partialread, reqlen):
        prefix = partialread
        while len(prefix) < reqlen:
            self._read_frame()
            prefix += self._rbuf.getvalue()
        self._rbuf = BytesIO(prefix)
        return self._rbuf


class _KerberosHiveMetastoreClient(ThriftClient):
    """Thrift HMS client backed by a Kerberos SASL transport."""

    def __init__(
        self,
        host: str,
        port: int,
        kerberos_service_name: str,
        kerberos_client_principal: Optional[str] = None,
    ) -> None:
        protocol = self._init_protocol(
            host, port, kerberos_service_name, kerberos_client_principal
        )
        super().__init__(protocol)

    @staticmethod
    def _init_protocol(
        host: str,
        port: int,
        kerberos_service_name: str,
        kerberos_client_principal: Optional[str],
    ):
        transport = _KerberosSaslTransport(
            TSocket.TSocket(host, int(port)),
            host=host,
            service=kerberos_service_name,
            principal=kerberos_client_principal,
        )
        return TBinaryProtocol.TBinaryProtocol(transport)

    def open(self) -> "_KerberosHiveMetastoreClient":
        self._oprot.trans.open()
        return self

    def close(self) -> None:
        self._oprot.trans.close()


class Hive3MetastoreClientWrapper:
    """Helper class to manage Hive 3.x Metastore client connections."""

    def __init__(
        self,
        uri: str,
        ugi: Optional[str] = None,
        *,
        sasl_enabled: bool = False,
        kerberos_service_name: str = "hive",
        kerberos_client_principal: Optional[str] = None,
    ):
        if not HIVE_AVAILABLE:
            raise ImportError(
                "Hive dependencies not installed. Please install with: "
                "pip install 'lance-namespace[hive3]'"
            )

        self._uri = uri
        self._ugi = ugi.split(":") if ugi else None
        self._sasl_enabled = sasl_enabled
        self._kerberos_service_name = kerberos_service_name
        self._kerberos_client_principal = kerberos_client_principal
        url_parts = urlparse(self._uri)
        self._host = url_parts.hostname or "localhost"
        self._port = url_parts.port or 9083
        self._client = None

    def __enter__(self):
        """Enter context manager."""
        if self._sasl_enabled:
            self._client = _KerberosHiveMetastoreClient(
                host=self._host,
                port=self._port,
                kerberos_service_name=self._kerberos_service_name,
                kerberos_client_principal=self._kerberos_client_principal,
            )
        else:
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
        self.sasl_enabled = _as_bool(properties.get(HIVE_METASTORE_SASL_ENABLED_KEY))
        self.metastore_kerberos_principal = properties.get(
            "hive.metastore.kerberos.principal"
        )
        self.kerberos_service_name = _get_kerberos_service_name(properties)
        self.kerberos_client_principal = properties.get("kerberos.client-principal")

        self._properties = properties.copy()
        self._client = None

    def namespace_id(self) -> str:
        """Return a human-readable unique identifier for this namespace instance."""
        return f"Hive3Namespace {{ uri: {self.uri!r} }}"

    @property
    def client(self):
        """Get the Hive client, initializing it if necessary."""
        if self._client is None:
            self._client = Hive3MetastoreClientWrapper(
                self.uri,
                self.ugi,
                sasl_enabled=self.sasl_enabled,
                kerberos_service_name=self.kerberos_service_name,
                kerberos_client_principal=self.kerberos_client_principal,
            )
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
        if catalog.lower() == "hive":
            # For default catalog, use hive2-compatible path
            return os.path.join(self.root, f"{database}.db", table)
        return os.path.join(self.root, catalog, f"{database}.db", table)

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
                if self.sasl_enabled:
                    properties[HIVE_METASTORE_SASL_ENABLED_KEY] = "true"
                    if self.metastore_kerberos_principal:
                        properties["hive.metastore.kerberos.principal"] = (
                            self.metastore_kerberos_principal
                        )
                    properties["kerberos.service-name"] = self.kerberos_service_name
                if self.kerberos_client_principal:
                    properties["kerberos.client-principal"] = (
                        self.kerberos_client_principal
                    )
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
                        "location", os.path.join(self.root, f"{database_name}.db")
                    )
                    if request.properties
                    else os.path.join(self.root, f"{database_name}.db")
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

        Only load_detailed_metadata=false is supported. Returns location only.
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

                return DescribeTableResponse(location=location)

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Table {request.id} does not exist")
            logger.error(f"Failed to describe table {request.id}: {e}")
            raise

    def drop_table(self, request: DropTableRequest) -> DropTableResponse:
        """Drop a table and delete its data."""
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

                client.drop_table(database, table_name, deleteData=True)

                return DropTableResponse(location=location)

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Table {request.id} does not exist")
            logger.error(f"Failed to drop table {request.id}: {e}")
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

    def declare_table(self, request: DeclareTableRequest) -> DeclareTableResponse:
        """Declare a table (metadata only)."""
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

            hive_table = HiveTable(
                tableName=table_name,
                dbName=database,
                sd=storage_descriptor,
                parameters=parameters,
                tableType="EXTERNAL_TABLE",
            )

            with self.client as client:
                client.create_table(hive_table)

            return DeclareTableResponse(location=location)

        except AlreadyExistsException:
            raise ValueError(f"Table {request.id} already exists")
        except Exception as e:
            logger.error(f"Failed to declare table {request.id}: {e}")
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
