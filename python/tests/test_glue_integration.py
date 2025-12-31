"""
Integration tests for AWS Glue namespace implementation.

To run these tests locally:
  1. Configure AWS credentials (via environment variables, ~/.aws/credentials, or IAM role)
  2. Run: make integ-test-glue

Tests are automatically skipped if AWS credentials are not available.
"""

import os
import tempfile
import uuid
import unittest

import pytest

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


def check_aws_credentials_available():
    """Check if AWS credentials are available."""
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True

    try:
        import boto3

        sts = boto3.client("sts", region_name=AWS_REGION)
        sts.get_caller_identity()
        return True
    except Exception:
        return False


aws_credentials_available = check_aws_credentials_available()


@pytest.mark.integration
@unittest.skipUnless(aws_credentials_available, "AWS credentials are not available")
class TestGlueNamespaceIntegration(unittest.TestCase):
    """Integration tests for GlueNamespace against a real AWS Glue catalog."""

    @classmethod
    def setUpClass(cls):
        """Set up class-level resources."""
        from lance_namespace_impls.glue import GlueNamespace

        cls.temp_dir = tempfile.mkdtemp(prefix="lance_glue_test_")
        cls.unique_id = uuid.uuid4().hex[:8]
        cls.test_database = f"lance_test_db_{cls.unique_id}"

        properties = {
            "region": AWS_REGION,
            "root": cls.temp_dir,
        }

        cls.namespace = GlueNamespace(**properties)

    @classmethod
    def tearDownClass(cls):
        """Clean up class-level resources."""
        if hasattr(cls, "namespace") and cls.namespace:
            try:
                cls._cleanup_database(cls.test_database)
            except Exception:
                pass

        import shutil

        if hasattr(cls, "temp_dir") and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir, ignore_errors=True)

    @classmethod
    def _cleanup_database(cls, database_name):
        """Helper to clean up a database and all its tables."""
        from lance_namespace_urllib3_client.models import (
            DropNamespaceRequest,
            ListTablesRequest,
            DeregisterTableRequest,
        )

        try:
            list_request = ListTablesRequest()
            list_request.id = [database_name]
            response = cls.namespace.list_tables(list_request)

            for table_name in response.tables:
                try:
                    dereg_request = DeregisterTableRequest()
                    dereg_request.id = [database_name, table_name]
                    cls.namespace.deregister_table(dereg_request)
                except Exception:
                    pass

            drop_request = DropNamespaceRequest()
            drop_request.id = [database_name]
            cls.namespace.drop_namespace(drop_request)
        except Exception:
            pass

    def setUp(self):
        """Set up test fixtures."""
        self.created_databases = []

    def tearDown(self):
        """Clean up test resources."""
        for db_name in self.created_databases:
            try:
                self._cleanup_database(db_name)
            except Exception:
                pass

    def _create_test_database(self, suffix=""):
        """Helper to create a test database with tracking for cleanup."""
        from lance_namespace_urllib3_client.models import CreateNamespaceRequest

        db_name = f"lance_test_{uuid.uuid4().hex[:8]}{suffix}"
        self.created_databases.append(db_name)

        create_request = CreateNamespaceRequest()
        create_request.id = [db_name]
        create_request.properties = {"description": "Lance integration test database"}
        self.namespace.create_namespace(create_request)
        return db_name

    def test_namespace_operations(self):
        """Test namespace (database) CRUD operations."""
        from lance_namespace_urllib3_client.models import (
            CreateNamespaceRequest,
            DescribeNamespaceRequest,
            DropNamespaceRequest,
            ListNamespacesRequest,
        )

        db_name = f"lance_test_{uuid.uuid4().hex[:8]}"
        self.created_databases.append(db_name)

        create_request = CreateNamespaceRequest()
        create_request.id = [db_name]
        create_request.properties = {"description": "Test database for Lance"}

        create_response = self.namespace.create_namespace(create_request)
        self.assertIsNotNone(create_response)

        describe_request = DescribeNamespaceRequest()
        describe_request.id = [db_name]

        describe_response = self.namespace.describe_namespace(describe_request)
        self.assertIsNotNone(describe_response)
        self.assertEqual(
            describe_response.properties.get("description"), "Test database for Lance"
        )

        list_request = ListNamespacesRequest()
        list_request.id = []
        list_response = self.namespace.list_namespaces(list_request)
        self.assertIn(db_name, list_response.namespaces)

        drop_request = DropNamespaceRequest()
        drop_request.id = [db_name]
        self.namespace.drop_namespace(drop_request)
        self.created_databases.remove(db_name)

    def test_table_operations(self):
        """Test table CRUD operations."""
        from lance_namespace_urllib3_client.models import (
            CreateEmptyTableRequest,
            DescribeTableRequest,
            DeregisterTableRequest,
            ListTablesRequest,
        )

        db_name = self._create_test_database()
        table_name = f"test_table_{uuid.uuid4().hex[:8]}"
        table_location = os.path.join(self.temp_dir, db_name, f"{table_name}.lance")

        # Create empty table (DeclareTable)
        create_request = CreateEmptyTableRequest()
        create_request.id = [db_name, table_name]
        create_request.location = table_location

        create_response = self.namespace.create_empty_table(create_request)
        self.assertIsNotNone(create_response.location)
        self.assertEqual(create_response.location, table_location)

        describe_request = DescribeTableRequest()
        describe_request.id = [db_name, table_name]

        describe_response = self.namespace.describe_table(describe_request)
        self.assertIsNotNone(describe_response.location)
        self.assertEqual(describe_response.location, table_location)

        list_request = ListTablesRequest()
        list_request.id = [db_name]

        list_response = self.namespace.list_tables(list_request)
        self.assertIn(table_name, list_response.tables)

        deregister_request = DeregisterTableRequest()
        deregister_request.id = [db_name, table_name]
        self.namespace.deregister_table(deregister_request)

    def test_multiple_tables_in_namespace(self):
        """Test creating and listing multiple tables in a namespace."""
        from lance_namespace_urllib3_client.models import (
            CreateEmptyTableRequest,
            DeregisterTableRequest,
            ListTablesRequest,
        )

        db_name = self._create_test_database()
        table_names = [f"table_{i}_{uuid.uuid4().hex[:6]}" for i in range(3)]

        for table_name in table_names:
            table_location = os.path.join(self.temp_dir, db_name, f"{table_name}.lance")
            create_request = CreateEmptyTableRequest()
            create_request.id = [db_name, table_name]
            create_request.location = table_location
            self.namespace.create_empty_table(create_request)

        list_request = ListTablesRequest()
        list_request.id = [db_name]
        list_response = self.namespace.list_tables(list_request)

        for table_name in table_names:
            self.assertIn(table_name, list_response.tables)

        for table_name in table_names:
            deregister_request = DeregisterTableRequest()
            deregister_request.id = [db_name, table_name]
            self.namespace.deregister_table(deregister_request)


if __name__ == "__main__":
    unittest.main()
