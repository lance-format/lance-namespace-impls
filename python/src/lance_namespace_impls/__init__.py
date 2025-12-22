# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""
Lance Namespace Implementations.

This package provides third-party catalog implementations for Lance Namespace:
- GlueNamespace: AWS Glue Data Catalog
- Hive2Namespace: Apache Hive 2.x Metastore
- IcebergNamespace: Apache Iceberg REST Catalog
- UnityNamespace: Unity Catalog
"""

from lance_namespace_impls.glue import GlueNamespace
from lance_namespace_impls.hive import Hive2Namespace
from lance_namespace_impls.iceberg import IcebergNamespace
from lance_namespace_impls.unity import UnityNamespace

__all__ = ["GlueNamespace", "Hive2Namespace", "IcebergNamespace", "UnityNamespace"]
