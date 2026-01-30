# Apache Gravitino

**Apache Gravitino** is a high-performance, geo-distributed, and federated metadata lake.
It manages metadata directly in different sources, types, and regions, providing unified metadata access for data and AI assets.

## Using Gravitino with Lance

Gravitino provides multiple ways to integrate with Lance, allowing you to choose the best option based on your use case.

### Option 1: Native Lance REST Support (Recommended)

Starting from version 1.1.0, Apache Gravitino provides **native integration** with the Lance REST Namespace.
This allows you to use Gravitino as a Lance namespace server directly, with full support for Lance tables.

#### Using the Lance Namespace Implementation

This repository provides native Lance namespace implementations for Gravitino in both Python and Java:

**Python Implementation:**
```python
from lance_namespace_impls import GravitinoNamespace

# Configure the namespace
namespace = GravitinoNamespace(
    endpoint="http://localhost:9101",  # Gravitino Lance REST service endpoint
    auth_token="your_token",           # Optional authentication token
    connect_timeout=10000,             # Connection timeout in milliseconds
    read_timeout=30000,                # Read timeout in milliseconds
    max_retries=3                      # Maximum retry attempts
)
```

**Java Implementation:**
```java
import org.lance.namespace.gravitino.GravitinoNamespace;

Map<String, String> config = new HashMap<>();
config.put("endpoint", "http://localhost:9101");
config.put("auth_token", "your_token");  // Optional

GravitinoNamespace namespace = new GravitinoNamespace();
namespace.initialize(config, allocator);
```

#### Configuration Properties

| Property | Required | Description | Default |
|----------|----------|-------------|---------|
| `endpoint` | Yes | Gravitino server endpoint (e.g., "http://localhost:9101") | - |
| `auth_token` | No | Authentication token for Gravitino | - |
| `connect_timeout` | No | Connection timeout in milliseconds | 10000 |
| `read_timeout` | No | Read timeout in milliseconds | 30000 |
| `max_retries` | No | Maximum retry attempts | 3 |

#### Namespace Hierarchy

The implementation follows Gravitino's three-level hierarchy:
- **Catalog** (first element of namespace identifier)
- **Schema** (second element of namespace identifier)
- **Table** (last element of table identifier)

Examples:
- Namespace: `["my_catalog", "my_schema"]`
- Table: `["my_catalog", "my_schema", "my_table"]`

For details on configuring Gravitino as a Lance REST namespace server, see the
[Gravitino Lance REST Service Documentation](https://gravitino.apache.org/docs/1.1.0/lance-rest-service).

### Option 2: Hive MetaStore

For users who primarily use Gravitino for its Hive MetaStore-related capabilities, Gravitino provides a
Hive MetaStore-compatible endpoint through its [Apache Hive Catalog](https://gravitino.apache.org/docs/0.6.1-incubating/apache-hive-catalog/).

Configure your Lance Hive namespace to connect to Gravitino's Hive MetaStore endpoint.
All the features and configurations of the Lance Hive Namespace ([V2](hive2.md) or [V3](hive3.md)) apply when using Gravitino's Hive catalog.
