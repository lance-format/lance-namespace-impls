"""
Integration tests for GooseFS Namespace implementation.

These tests run against a real GooseFS Table Master. They are skipped when:
- The `goosefs_metastore_client` extra is not installed, or
- The Table Master is not reachable at GOOSEFS_HOST:GOOSEFS_PORT.

All operations are anchored under a single root namespace
(defaults to `root`, override with GOOSEFS_ROOT_NAMESPACE). The root must
already be attached on the server; the suite never tries to create or drop
it. Every test database/table id is `[ROOT_NAMESPACE, ...]`, so the suite
exercises real write paths via the existing catalog-routed code path on
the server.

Environment:
  GOOSEFS_HOST            default: localhost
  GOOSEFS_PORT            default: 9220
  GOOSEFS_ROOT_NAMESPACE  default: root  (must be attached on the server)
"""

import os
import socket
import uuid
import unittest

import pytest

from lance_namespace_impls.goosefs import GooseFSNamespace, GOOSEFS_CLIENT_AVAILABLE
from lance_namespace_urllib3_client.models import (
    CreateNamespaceRequest,
    DeclareTableRequest,
    DescribeNamespaceRequest,
    DropNamespaceRequest,
    ListNamespacesRequest,
    ListTablesRequest,
)


GOOSEFS_HOST = os.environ.get("GOOSEFS_HOST", "localhost")
GOOSEFS_PORT = int(os.environ.get("GOOSEFS_PORT", "9220"))
GOOSEFS_URI = f"goosefs://{GOOSEFS_HOST}:{GOOSEFS_PORT}"
ROOT_NAMESPACE = os.environ.get("GOOSEFS_ROOT_NAMESPACE", "root")


def check_goosefs_available():
    """Check if GooseFS Table Master is reachable on TCP."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((GOOSEFS_HOST, GOOSEFS_PORT))
        sock.close()
        return result == 0
    except Exception:
        return False


goosefs_available = check_goosefs_available()


@pytest.mark.integration
@unittest.skipUnless(
    GOOSEFS_CLIENT_AVAILABLE and goosefs_available,
    f"GooseFS dependencies not installed or Table Master not available at {GOOSEFS_URI}",
)
class TestGooseFSNamespaceIntegration(unittest.TestCase):
    """Integration tests for GooseFSNamespace against a running GooseFS Table Master.

    All ids are scoped under the pre-attached root namespace
    `ROOT_NAMESPACE` (default `"root"`).
    """

    def setUp(self):
        unique = uuid.uuid4().hex[:8]
        self.test_database = f"test_db_{unique}"
        # Every operation is anchored under the pre-attached root namespace.
        self.db_id = [ROOT_NAMESPACE, self.test_database]
        # `manifest_enabled` + `dir_listing_enabled` are connection-level
        # GooseFS namespace defaults; the namespace impl auto-merges them
        # into every CreateNamespaceRequest.properties, so a writable root
        # configured for manifest mode is enough to make the write paths
        # succeed.
        self.namespace = GooseFSNamespace(
            uri=GOOSEFS_URI,
            manifest_enabled="true",
            dir_listing_enabled="true",
        )
        # Set to True by `_create_test_database` after a successful create,
        # so `tearDown` only attempts cleanup when there is actually
        # something to clean up.
        self._created = False

    def tearDown(self):
        if self._created:
            try:
                self.namespace.drop_namespace(DropNamespaceRequest(id=self.db_id))
            except Exception:
                pass

        if self.namespace:
            try:
                self.namespace.close()
            except Exception:
                pass

    def _create_test_database(self):
        """Create the test database under the root namespace.

        Skips with a clear backend-mismatch message if the server rejects
        the create with a recognisable legacy error.
        """
        try:
            response = self.namespace.create_namespace(
                CreateNamespaceRequest(id=self.db_id, properties={})
            )
            self._created = True
            return response
        except Exception as exc:
            msg = str(exc)
            # Legacy server signals (predates the catalog-removal change).
            if "Catalog" in msg or "catalogName" in msg:
                self.skipTest(
                    f"Server rejected create under {ROOT_NAMESPACE!r} "
                    f"({msg[:120]}). Confirm the root namespace is attached "
                    "and writable."
                )
            # `root` namespace exists but isn't configured for child namespaces.
            if "manifest mode" in msg or "Failed to create namespace" in msg:
                self.skipTest(
                    f"Root namespace {ROOT_NAMESPACE!r} does not allow child "
                    f"namespace creates ({msg[:120]}). Enable manifest mode "
                    "on the root namespace or set GOOSEFS_ROOT_NAMESPACE to a "
                    "writable root."
                )
            raise

    def test_list_root_attached(self):
        """The configured root namespace must be attached on the server."""
        response = self.namespace.list_namespaces(ListNamespacesRequest(id=[]))
        self.assertIsInstance(response.namespaces, list)
        self.assertIn(
            ROOT_NAMESPACE,
            response.namespaces,
            f"Root namespace {ROOT_NAMESPACE!r} is not attached on the server "
            f"(found: {response.namespaces})",
        )

    def test_describe_root(self):
        """Describing the root namespace must succeed."""
        response = self.namespace.describe_namespace(
            DescribeNamespaceRequest(id=[ROOT_NAMESPACE])
        )
        self.assertIsNotNone(response.properties)
        self.assertIsInstance(response.properties, dict)

    def test_list_databases_under_root(self):
        """Listing under root returns the databases it contains."""
        response = self.namespace.list_namespaces(
            ListNamespacesRequest(id=[ROOT_NAMESPACE])
        )
        self.assertIsInstance(response.namespaces, list)

    def test_namespace_operations(self):
        """Create → describe → list → drop a database under root."""
        create_response = self._create_test_database()
        self.assertIsNotNone(create_response)

        describe_response = self.namespace.describe_namespace(
            DescribeNamespaceRequest(id=self.db_id)
        )
        self.assertIsNotNone(describe_response)
        self.assertIsInstance(describe_response.properties, dict)

        list_response = self.namespace.list_namespaces(
            ListNamespacesRequest(id=[ROOT_NAMESPACE])
        )
        self.assertIn(self.test_database, list_response.namespaces)

        # tearDown handles the drop, but exercise it here too so failures
        # surface immediately.
        self.namespace.drop_namespace(DropNamespaceRequest(id=self.db_id))
        # Mark cleaned up so tearDown does not try to drop again.
        self._created = False

    def test_table_operations(self):
        """Declare → list a table under a freshly-created database.

        On this server `declare_table` requires a server-assigned location
        of the form
          ``goosefs://<master>/<root_uri>/<prefix>_<db>$<table>``
        where ``<prefix>`` is a server-generated hex string the client
        cannot predict. The server *does* know the right location and
        reveals it in its own logs (``Cannot declare table … must be at
        location <X>``), but the gRPC error message that reaches the
        client is just ``Failed to declare table [<db>, <table>]`` —
        ``X`` is dropped on the way back. Until the server propagates
        that detail (or grows an API to query the assigned location), we
        cannot exercise the declare path against the live server.
        """
        self._create_test_database()

        table_name = f"test_table_{uuid.uuid4().hex[:8]}"
        table_id = self.db_id + [table_name]

        try:
            self.namespace.declare_table(
                DeclareTableRequest(id=table_id, location="/placeholder")
            )
        except Exception as exc:
            self.skipTest(
                f"declare_table requires a server-assigned location that is "
                f"not surfaced to the client. Server error: {str(exc)[:160]}"
            )

        # If we ever land here the server has either started auto-assigning
        # locations or stopped enforcing the rule. Keep the rest of the
        # test in place so it surfaces that change as a real signal.
        list_response = self.namespace.list_tables(ListTablesRequest(id=self.db_id))
        self.assertIn(table_name, list_response.tables)

    def test_create_namespace_returns_properties(self):
        """`create_namespace` response carries a properties mapping."""
        response = self._create_test_database()
        self.assertIsNotNone(response)
        self.assertIsNotNone(response.properties)
        self.assertIsInstance(response.properties, dict)


if __name__ == "__main__":
    unittest.main()
