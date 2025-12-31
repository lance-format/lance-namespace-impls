#!/bin/sh
# Wait for Polaris to be healthy
echo "Waiting for Polaris to be ready..."
until wget -q -O /dev/null http://polaris:8182/q/health 2>/dev/null; do
  echo "Polaris not ready, waiting..."
  sleep 2
done
echo "Polaris is healthy!"

# Get OAuth token
echo "Getting OAuth token..."
TOKEN_RESPONSE=$(wget -q -O - --post-data='grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL' \
  --header='Content-Type: application/x-www-form-urlencoded' \
  http://polaris:8181/api/catalog/v1/oauth/tokens)

TOKEN=$(echo "$TOKEN_RESPONSE" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

if [ -z "$TOKEN" ]; then
  echo "Failed to get OAuth token"
  echo "Response: $TOKEN_RESPONSE"
  exit 1
fi
echo "Token obtained successfully"

# Check if catalog already exists
echo "Checking for existing catalog..."
CATALOGS=$(wget -q -O - --header="Authorization: Bearer $TOKEN" \
  http://polaris:8181/api/management/v1/catalogs)

if echo "$CATALOGS" | grep -q '"name":"test_catalog"'; then
  echo "Catalog 'test_catalog' already exists"
  exit 0
fi

# Create test catalog
echo "Creating test catalog..."
RESULT=$(wget -q -O - --post-data='{
  "catalog": {
    "name": "test_catalog",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "file:///data/warehouse/test_catalog"
    },
    "storageConfigInfo": {
      "storageType": "FILE",
      "allowedLocations": ["file:///data/warehouse"]
    }
  }
}' \
  --header="Authorization: Bearer $TOKEN" \
  --header='Content-Type: application/json' \
  http://polaris:8181/api/management/v1/catalogs 2>&1)

echo "Result: $RESULT"

# Verify catalog was created
VERIFY=$(wget -q -O - --header="Authorization: Bearer $TOKEN" \
  http://polaris:8181/api/management/v1/catalogs)

if echo "$VERIFY" | grep -q '"name":"test_catalog"'; then
  echo "Catalog 'test_catalog' created successfully!"
  exit 0
else
  echo "Failed to create catalog"
  echo "Catalogs: $VERIFY"
  exit 1
fi
