# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""
Lance Namespace Implementations.

This package provides third-party catalog implementations for Lance Namespace:
- GlueNamespace: AWS Glue Data Catalog
- GravitinoNamespace: Apache Gravitino Lance REST Service
- Hive2Namespace: Apache Hive 2.x Metastore
- Hive3Namespace: Apache Hive 3.x Metastore (with catalog support)
- IcebergNamespace: Apache Iceberg REST Catalog
- PolarisNamespace: Apache Polaris Catalog
- UnityNamespace: Unity Catalog

Shared infrastructure:
- RestClient: Reusable HTTP client for REST API implementations
- RestClientException: Exception raised by RestClient
- NamespaceException: Base exception for namespace operations
"""

from lance_namespace_impls.glue import GlueNamespace
from lance_namespace_impls.gravitino import GravitinoNamespace
from lance_namespace_impls.hive2 import Hive2Namespace
from lance_namespace_impls.hive3 import Hive3Namespace
from lance_namespace_impls.iceberg import IcebergNamespace
from lance_namespace_impls.polaris import PolarisNamespace
from lance_namespace_impls.unity import UnityNamespace
from lance_namespace_impls.rest_client import (
    RestClient,
    RestClientException,
    NamespaceException,
    NamespaceNotFoundException,
    NamespaceAlreadyExistsException,
    TableNotFoundException,
    TableAlreadyExistsException,
    InvalidInputException,
    InternalException,
)

__all__ = [
    "GlueNamespace",
    "GravitinoNamespace",
    "Hive2Namespace",
    "Hive3Namespace",
    "IcebergNamespace",
    "PolarisNamespace",
    "UnityNamespace",
    "RestClient",
    "RestClientException",
    "NamespaceException",
    "NamespaceNotFoundException",
    "NamespaceAlreadyExistsException",
    "TableNotFoundException",
    "TableAlreadyExistsException",
    "InvalidInputException",
    "InternalException",
]
