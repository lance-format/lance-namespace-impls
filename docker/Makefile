# Docker Makefile for managing catalog services
#
# Usage:
#   make setup       - Download required dependencies (PostgreSQL driver)
#   make up          - Start all services
#   make down        - Stop all services
#   make down-clean  - Stop all services and remove volumes
#   make logs        - View logs from all services
#   make status      - Check service status
#
#   make up-hive2    - Start only Hive 2.x
#   make up-hive3    - Start only Hive 3.x
#   make up-polaris  - Start only Polaris
#   make up-unity    - Start only Unity Catalog
#

POSTGRES_DRIVER_VERSION := 42.7.3
POSTGRES_DRIVER_URL := https://jdbc.postgresql.org/download/postgresql-$(POSTGRES_DRIVER_VERSION).jar

.PHONY: setup up down down-clean logs status health
.PHONY: up-hive2 down-hive2 up-hive3 down-hive3 up-polaris down-polaris up-unity down-unity

# Download PostgreSQL JDBC driver for Hive
setup:
	@echo "Downloading PostgreSQL JDBC driver..."
	@curl -sSL -o hive2/postgresql-$(POSTGRES_DRIVER_VERSION).jar $(POSTGRES_DRIVER_URL) || true
	@curl -sSL -o hive3/postgresql-$(POSTGRES_DRIVER_VERSION).jar $(POSTGRES_DRIVER_URL) || true
	@echo "Setup complete."

# Start all services
up: setup
	docker compose -f docker-compose.yml up -d

# Stop all services
down:
	docker compose -f docker-compose.yml down

# Stop all services and remove volumes
down-clean:
	docker compose -f docker-compose.yml down -v

# View logs from all services
logs:
	docker compose -f docker-compose.yml logs -f

# Check service status
status:
	docker compose -f docker-compose.yml ps

# Health check all services
health:
	@echo "=== Hive 2.x Metastore (port 9083) ==="
	@nc -z localhost 9083 && echo "OK" || echo "NOT READY"
	@echo ""
	@echo "=== Hive 3.x Metastore (port 9084) ==="
	@nc -z localhost 9084 && echo "OK" || echo "NOT READY"
	@echo ""
	@echo "=== Polaris (port 8181) ==="
	@curl -sf http://localhost:8182/q/health > /dev/null && echo "OK" || echo "NOT READY"
	@echo ""
	@echo "=== Unity Catalog (port 8080) ==="
	@curl -sf http://localhost:8080/api/2.1/unity-catalog/catalogs > /dev/null && echo "OK" || echo "NOT READY"

# ============================================================================
# Individual service management
# ============================================================================

# Hive 2.x
up-hive2: setup
	docker compose -f hive2/docker-compose.yml up -d

down-hive2:
	docker compose -f hive2/docker-compose.yml down

down-hive2-clean:
	docker compose -f hive2/docker-compose.yml down -v

logs-hive2:
	docker compose -f hive2/docker-compose.yml logs -f

# Hive 3.x
up-hive3: setup
	docker compose -f hive3/docker-compose.yml up -d

down-hive3:
	docker compose -f hive3/docker-compose.yml down

down-hive3-clean:
	docker compose -f hive3/docker-compose.yml down -v

logs-hive3:
	docker compose -f hive3/docker-compose.yml logs -f

# Polaris
up-polaris:
	docker compose -f polaris/docker-compose.yml up -d

down-polaris:
	docker compose -f polaris/docker-compose.yml down

down-polaris-clean:
	docker compose -f polaris/docker-compose.yml down -v

logs-polaris:
	docker compose -f polaris/docker-compose.yml logs -f

# Unity Catalog
up-unity:
	docker compose -f unity/docker-compose.yml up -d

down-unity:
	docker compose -f unity/docker-compose.yml down

down-unity-clean:
	docker compose -f unity/docker-compose.yml down -v

logs-unity:
	docker compose -f unity/docker-compose.yml logs -f

# ============================================================================
# Test utilities
# ============================================================================

# Get Polaris access token
polaris-token:
	@curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
		-H 'Content-Type: application/x-www-form-urlencoded' \
		-d 'grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL' | \
		python3 -c "import sys, json; print(json.load(sys.stdin).get('access_token', 'ERROR'))"

# Test Polaris catalog creation
polaris-create-catalog:
	@TOKEN=$$(make -s polaris-token) && \
	curl -s -X POST http://localhost:8181/api/catalog/v1/catalogs \
		-H "Authorization: Bearer $$TOKEN" \
		-H 'Content-Type: application/json' \
		-d '{"name": "lance_test", "type": "INTERNAL", "properties": {"default-base-location": "file:///data/warehouse/lance_test"}, "storageConfigInfo": {"storageType": "FILE"}}'

# Test Unity Catalog
unity-list-catalogs:
	@curl -s http://localhost:8080/api/2.1/unity-catalog/catalogs | python3 -m json.tool

# Create Unity test catalog
unity-create-catalog:
	@curl -s -X POST http://localhost:8080/api/2.1/unity-catalog/catalogs \
		-H 'Content-Type: application/json' \
		-d '{"name": "lance_test", "comment": "Test catalog for Lance tables"}'
