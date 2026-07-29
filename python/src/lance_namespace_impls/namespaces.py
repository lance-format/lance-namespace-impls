# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""
LanceNamespaces - Unified factory for Lance namespace implementations.

Usage:
    from lance_namespace_impls import LanceNamespaces

    # Connect to GooseFS Lance Namespace
    namespace = LanceNamespaces.connect("goosefs", {
        "uri": "goosefs://master:9220",
        "root": "/my/dir",
    })

    # Connect to Hive3
    namespace = LanceNamespaces.connect("hive3", {
        "uri": "thrift://localhost:9083",
        "root": "/data/lance",
    })
"""

from typing import Dict, List, Optional, Any, Type

from lance.namespace import LanceNamespace


class LanceNamespaces:
    """
    Factory class for creating Lance namespace instances.

    This class provides a unified API to connect to different Lance namespace
    implementations, similar to Java's LanceNamespaces.connect() pattern.

    Example:
        >>> from lance_namespace_impls import LanceNamespaces
        >>>
        >>> # Connect to GooseFS
        >>> namespace = LanceNamespaces.connect("goosefs", {
        ...     "uri": "goosefs://master:9220",
        ...     "root": "/data/lance",
        ... })
        >>>
        >>> # List databases
        >>> from lance_namespace_urllib3_client.models import ListNamespacesRequest
        >>> response = namespace.list_namespaces(ListNamespacesRequest())
        >>> print(response.namespaces)
    """

    # Registry of namespace implementations (lazy loaded)
    _registry: Optional[Dict[str, Type[LanceNamespace]]] = None

    @classmethod
    def _get_registry(cls) -> Dict[str, Type[LanceNamespace]]:
        """Get or initialize the namespace registry."""
        if cls._registry is None:
            # Import here to avoid circular imports
            from lance_namespace_impls.glue import GlueNamespace
            from lance_namespace_impls.goosefs import GooseFSNamespace
            from lance_namespace_impls.hive2 import Hive2Namespace
            from lance_namespace_impls.hive3 import Hive3Namespace
            from lance_namespace_impls.iceberg import IcebergNamespace
            from lance_namespace_impls.polaris import PolarisNamespace
            from lance_namespace_impls.unity import UnityNamespace

            cls._registry = {
                "glue": GlueNamespace,
                "goosefs": GooseFSNamespace,
                "hive2": Hive2Namespace,
                "hive3": Hive3Namespace,
                "iceberg": IcebergNamespace,
                "polaris": PolarisNamespace,
                "unity": UnityNamespace,
            }
        return cls._registry

    @classmethod
    def connect(
        cls,
        namespace_type: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> LanceNamespace:
        """
        Connect to a Lance namespace using the specified implementation.

        Args:
            namespace_type: Type of namespace to connect to. Supported types:
                - "goosefs": GooseFS Table Master
                - "glue": AWS Glue Data Catalog
                - "hive2": Apache Hive 2.x Metastore
                - "hive3": Apache Hive 3.x Metastore
                - "iceberg": Apache Iceberg REST Catalog
                - "polaris": Apache Polaris Catalog
                - "unity": Unity Catalog

            properties: Connection properties (varies by namespace type).

                For GooseFS:
                    - uri: GooseFS URI (e.g., "goosefs://master:9220")
                    - root: Storage root location for Lance tables
                    - connect_timeout: Connection timeout in ms (default: 10000)
                    - read_timeout: Read timeout in ms (default: 30000)

                For Hive2/Hive3:
                    - uri: Hive Metastore Thrift URI (e.g., "thrift://localhost:9083")
                    - root: Storage root location
                    - ugi: User Group Information (optional)

                For Iceberg:
                    - endpoint: REST catalog endpoint URL
                    - warehouse: Warehouse name
                    - token: Authentication token (optional)

                For Unity:
                    - endpoint: Unity Catalog endpoint URL
                    - token: Authentication token

                For Polaris:
                    - endpoint: Polaris Catalog endpoint URL
                    - auth_token: Authentication token

                For Glue:
                    - region: AWS region
                    - access_key_id: AWS access key (optional)
                    - secret_access_key: AWS secret key (optional)

        Returns:
            A LanceNamespace instance connected to the specified catalog.

        Raises:
            ValueError: If namespace_type is not supported.

        Examples:
            >>> # Connect to GooseFS
            >>> namespace = LanceNamespaces.connect("goosefs", {
            ...     "uri": "goosefs://master:9220",
            ...     "root": "/data/lance",
            ... })

            >>> # Connect to Hive2
            >>> namespace = LanceNamespaces.connect("hive2", {
            ...     "uri": "thrift://localhost:9083",
            ...     "root": "/my/dir",
            ...     "ugi": "user:group1,group2",
            ... })
        """
        registry = cls._get_registry()
        namespace_type = namespace_type.lower()

        if namespace_type not in registry:
            supported = ", ".join(sorted(registry.keys()))
            raise ValueError(
                f"Unsupported namespace type: {namespace_type}. "
                f"Supported types: {supported}"
            )

        props = dict(properties) if properties else {}
        namespace_class = registry[namespace_type]
        return namespace_class(**props)

    @classmethod
    def register(cls, name: str, namespace_class: Type[LanceNamespace]) -> None:
        """
        Register a custom namespace implementation.

        Args:
            name: Name to register the namespace under
            namespace_class: Namespace implementation class

        Example:
            >>> class MyCustomNamespace(LanceNamespace):
            ...     def __init__(self, **props):
            ...         pass
            >>> LanceNamespaces.register("custom", MyCustomNamespace)
            >>> namespace = LanceNamespaces.connect("custom", {"key": "value"})
        """
        registry = cls._get_registry()
        registry[name.lower()] = namespace_class

    @classmethod
    def available(cls) -> List[str]:
        """
        List all available namespace implementations.

        Returns:
            List of registered namespace type names.

        Example:
            >>> print(LanceNamespaces.available())
            ['glue', 'goosefs', 'hive2', 'hive3', 'iceberg', 'polaris', 'unity']
        """
        return sorted(cls._get_registry().keys())
