"""
Example demonstrating how to use ThreeLevelNamespaceAdapter.

This example shows how to wrap any namespace implementation with the adapter
to enforce 3-level table identifiers.
"""

from lance_namespace_impls import ThreeLevelNamespaceAdapter
from lance_namespace_urllib3_client.models import (
    DeclareTableRequest,
    ListNamespacesRequest,
    CreateNamespaceRequest,
)


def example_with_hive3():
    """Example: Wrap a Hive3 namespace with 3-level enforcement."""
    # Uncomment to use with actual Hive3 namespace
    # from lance_namespace_impls import Hive3Namespace
    #
    # # Create underlying Hive3 namespace
    # hive3 = Hive3Namespace(uri="thrift://localhost:9083")
    #
    # # Wrap with 3-level adapter
    # enforced = ThreeLevelNamespaceAdapter(hive3)
    #
    # # Now all table operations require exactly 3 levels
    # request = DeclareTableRequest(
    #     id=["catalog", "database", "table"],  # Must be exactly 3 levels
    #     var_schema=schema
    # )
    # enforced.declare_table(request, dataset_bytes)
    #
    # # This would fail with ValueError:
    # # bad_request = DeclareTableRequest(
    # #     id=["database", "table"],  # Only 2 levels - rejected!
    # #     var_schema=schema
    # # )
    # # enforced.declare_table(bad_request, dataset_bytes)
    pass


def example_with_glue():
    """Example: Wrap a Glue namespace with 3-level enforcement."""
    # Uncomment to use with actual Glue namespace
    # from lance_namespace_impls import GlueNamespace
    #
    # # Create underlying Glue namespace
    # glue = GlueNamespace(region="us-east-1")
    #
    # # Wrap with 3-level adapter
    # enforced = ThreeLevelNamespaceAdapter(glue)
    #
    # # Namespace operations allow up to 2 levels
    # ns_request = ListNamespacesRequest(
    #     id=["catalog", "database"]  # Up to 2 levels allowed
    # )
    # enforced.list_namespaces(ns_request)
    #
    # # Create a namespace with 2 levels
    # create_request = CreateNamespaceRequest(
    #     id=["catalog", "database"],
    #     properties={"description": "My database"}
    # )
    # enforced.create_namespace(create_request)
    pass


def example_with_polaris():
    """Example: Wrap a Polaris namespace with 3-level enforcement."""
    # Uncomment to use with actual Polaris namespace
    # from lance_namespace_impls import PolarisNamespace
    #
    # # Create underlying Polaris namespace (supports unlimited nesting)
    # polaris = PolarisNamespace(
    #     endpoint="http://localhost:8181",
    #     auth_token="your-token"
    # )
    #
    # # Wrap with 3-level adapter to enforce strict hierarchy
    # enforced = ThreeLevelNamespaceAdapter(polaris)
    #
    # # Now Polaris is restricted to 3-level tables
    # request = DeclareTableRequest(
    #     id=["catalog", "database", "table"],
    #     var_schema=schema
    # )
    # enforced.declare_table(request, dataset_bytes)
    #
    # # This would fail even though Polaris supports it:
    # # deep_request = DeclareTableRequest(
    # #     id=["catalog", "db1", "db2", "table"],  # 4 levels - rejected!
    # #     var_schema=schema
    # # )
    # # enforced.declare_table(deep_request, dataset_bytes)
    pass


def main():
    """Run all examples."""
    print("ThreeLevelNamespaceAdapter Examples")
    print("=" * 50)
    print()
    print("These examples show how to wrap different namespace")
    print("implementations with the ThreeLevelNamespaceAdapter.")
    print()
    print("Uncomment the code in each example function to try them out.")
    print()
    print("Example 1: Hive3 Namespace")
    example_with_hive3()
    print()
    print("Example 2: Glue Namespace")
    example_with_glue()
    print()
    print("Example 3: Polaris Namespace")
    example_with_polaris()


if __name__ == "__main__":
    main()
