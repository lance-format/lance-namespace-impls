# Docker Setup for Catalog Integration Testing

This directory contains Docker Compose configurations for running Hive 2.x, Hive 3.x, Apache Polaris, and Unity Catalog locally for integration testing.

## Prerequisites

- Docker and Docker Compose installed
- `curl` for downloading dependencies
- `nc` (netcat) for health checks (optional)

## Quick Start

### Start All Services

```bash
cd docker
make setup  # Download PostgreSQL driver for Hive
make up     # Start all services
make health # Check if all services are ready
```

### Start Individual Services

```bash
make up-hive2    # Start Hive 2.x Metastore only
make up-hive3    # Start Hive 3.x/4.x Metastore only
make up-polaris  # Start Apache Polaris only
make up-unity    # Start Unity Catalog only
```

### Stop Services

```bash
make down        # Stop all services (keep data)
make down-clean  # Stop all services and remove data volumes
```

## Service Endpoints

| Service | Port | Description |
|---------|------|-------------|
| Hive 2.x Metastore | 9083 | Thrift metastore service |
| Hive 3.x Metastore | 9084 | Thrift metastore service |
| Hive 3.x Catalog | 9001 | Catalog servlet endpoint |
| Polaris API | 8181 | REST API endpoint |
| Polaris Management | 8182 | Health and metrics |
| Unity Catalog | 8080 | REST API endpoint |

## Service Details

### Hive 2.x Metastore

- **Image**: `apache/hive:2.3.9`
- **Thrift Port**: 9083
- **Database**: PostgreSQL on port 5432
- **Credentials**: `hive/hive`

```java
// Java connection example
HiveConf conf = new HiveConf();
conf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:9083");
IMetaStoreClient client = new HiveMetaStoreClient(conf);
```

### Hive 3.x/4.x Metastore

- **Image**: `apache/hive:4.0.0`
- **Thrift Port**: 9084
- **Catalog Servlet Port**: 9001
- **Database**: PostgreSQL on port 5433
- **Credentials**: `hive/hive`

Hive 3.x adds catalog support for multi-catalog environments.

```java
// Java connection example
HiveConf conf = new HiveConf();
conf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:9084");
IMetaStoreClient client = new HiveMetaStoreClient(conf);
```

### Apache Polaris

- **Image**: `apache/polaris:latest`
- **API Port**: 8181
- **Management Port**: 8182
- **Database**: PostgreSQL on port 5434
- **Default Credentials**: `root:s3cr3t` (realm: POLARIS)

#### Authentication

Polaris uses OAuth2 client credentials flow:

```bash
# Get access token
curl -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL'

# Use token in requests
curl -H "Authorization: Bearer <token>" \
  http://localhost:8181/api/catalog/v1/namespaces
```

#### Create a Catalog

```bash
make polaris-token        # Get token
make polaris-create-catalog  # Create test catalog
```

Or manually:

```bash
TOKEN=$(make -s polaris-token)
curl -X POST http://localhost:8181/api/catalog/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "lance_test",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "file:///data/warehouse/lance_test"
    },
    "storageConfigInfo": {
      "storageType": "FILE"
    }
  }'
```

### Unity Catalog

- **Image**: `unitycatalog/unitycatalog:latest`
- **API Port**: 8080
- **Authorization**: Disabled for testing

Unity Catalog uses a simple REST API without authentication in dev mode.

```bash
# List catalogs
curl http://localhost:8080/api/2.1/unity-catalog/catalogs

# Create catalog
curl -X POST http://localhost:8080/api/2.1/unity-catalog/catalogs \
  -H 'Content-Type: application/json' \
  -d '{"name": "lance_test", "comment": "Test catalog"}'

# List schemas
curl http://localhost:8080/api/2.1/unity-catalog/schemas?catalog_name=lance_test
```

## Running Integration Tests

### Java Tests

Configure your test properties:

```properties
# Hive 2.x
hive2.metastore.uri=thrift://localhost:9083

# Hive 3.x
hive3.metastore.uri=thrift://localhost:9084

# Polaris
polaris.endpoint=http://localhost:8181
polaris.auth_token=<get from make polaris-token>

# Unity Catalog
unity.endpoint=http://localhost:8080
unity.catalog=lance_test
```

Run tests:

```bash
# From project root
cd java
./mvnw test -Dtest=*IntegrationTest -DskipUnitTests
```

### Python Tests

```python
# Hive 2.x
from lance_namespace_impls.hive import HiveNamespace

ns = HiveNamespace({
    "metastore_uri": "thrift://localhost:9083"
})

# Polaris
from lance_namespace_impls.polaris import PolarisNamespace

ns = PolarisNamespace({
    "endpoint": "http://localhost:8181",
    "auth_token": "<token>"
})

# Unity
from lance_namespace_impls.unity import UnityNamespace

ns = UnityNamespace({
    "endpoint": "http://localhost:8080",
    "catalog": "lance_test"
})
```

## Troubleshooting

### Services Not Starting

Check logs:

```bash
make logs          # All services
make logs-hive3    # Specific service
```

### Port Conflicts

If ports are already in use, modify the port mappings in the docker-compose files:

```yaml
ports:
  - "9085:9083"  # Change host port
```

### Hive Metastore Schema Issues

If metastore fails to initialize schema:

```bash
# Reset Hive 3.x
make down-hive3-clean
make up-hive3
```

### Polaris Authentication Errors

Ensure you're using the correct credentials:

- Realm: `POLARIS`
- Client ID: `root`
- Client Secret: `s3cr3t`

## Persistence

Data is stored in Docker volumes:

- `hive2-postgres-data`, `hive2-warehouse`
- `hive3-postgres-data`, `hive3-warehouse`
- `polaris-postgres-data`, `polaris-warehouse`
- `unity-data`

To reset all data:

```bash
make down-clean
```
