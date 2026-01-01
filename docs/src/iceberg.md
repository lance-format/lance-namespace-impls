# Lance Iceberg REST Catalog Implementation Spec

This document describes how the Apache Iceberg REST Catalog implements the Lance Namespace client spec.

## Background

Apache Iceberg REST Catalog is a standardized REST API for interacting with Iceberg catalogs. It provides a vendor-neutral interface for managing tables and namespaces across different catalog backends. When registering a Lance table, the implementation creates a companion Iceberg table with a dummy schema at the same location, using table properties to identify it as a Lance table. For details on the Iceberg REST Catalog, see the [Iceberg REST Catalog Specification](https://iceberg.apache.org/docs/latest/rest-spec/).

## Namespace Implementation Configuration Properties

The Lance Iceberg REST Catalog namespace implementation accepts the following configuration properties:

The **endpoint** property is required and specifies the Iceberg REST Catalog server endpoint URL (e.g., `http://localhost:8181`). Must start with `http://` or `https://`.

The **warehouse** property is optional and specifies the warehouse identifier. Some Iceberg REST implementations require this. The warehouse name is resolved to an API prefix via the `/v1/config` endpoint.

The **auth_token** property is optional and specifies the bearer token for authentication.

The **credential** property is optional and specifies the OAuth2 client credential in `client_id:client_secret` format for client credentials authentication flow.

The **connect_timeout** property is optional and specifies the connection timeout in milliseconds (default: 10000).

The **read_timeout** property is optional and specifies the read timeout in milliseconds (default: 30000).

The **max_retries** property is optional and specifies the maximum number of retries for failed requests (default: 3).

The **root** property is optional and specifies the default storage root location for tables. When not specified, it defaults to the current working directory.

## Object Mapping

### Warehouse

The **warehouse** is the first element of any namespace or table identifier. The implementation caches the warehouse to config mapping by calling the `/v1/config?warehouse={warehouse}` endpoint. If the config response contains a `prefix` in the defaults, that prefix is used for API paths; otherwise, the warehouse name itself is used as the prefix.

### Namespace

The **root namespace** is represented by specifying only the warehouse in the identifier (e.g., `["my-warehouse"]`). This lists all top-level namespaces under that warehouse.

A **child namespace** is a nested namespace in Iceberg. Iceberg supports arbitrary nesting depth. The **namespace identifier** format is `[warehouse, namespace1, namespace2, ...]` (e.g., `["my-warehouse", "level1", "level2"]`).

For API calls, namespace levels (excluding the warehouse) are joined with the `\x1F` (unit separator) character. In user-facing contexts, a `.` delimiter is used.

**Namespace properties** are stored in the namespace's properties map, returned by the Iceberg namespace API.

### Table

A **table** is represented as a regular Iceberg table with a dummy schema. The dummy schema contains a single nullable string column named `dummy`. This approach ensures compatibility with the Iceberg REST Catalog API while storing the actual Lance table at the same location.

The **table identifier** format is `[warehouse, namespace1, namespace2, ..., table_name]` (e.g., `["my-warehouse", "db", "my_table"]`).

The **table location** is stored in the `location` field of the Iceberg table metadata, pointing to the root location of the Lance table.

**Table properties** are stored in the Iceberg table's `properties` map.

## Lance Table Identification

A table in Iceberg REST Catalog is identified as a Lance table when the `properties` map contains a key `table_type` with value `lance` (case insensitive). The `location` must point to a valid Lance table root directory. The Iceberg table itself serves as a metadata wrapper, with the actual data stored in Lance format.

## Basic Operations

### CreateNamespace

Creates a new namespace in the Iceberg catalog.

The implementation:

1. Extract the warehouse from the first element of the namespace identifier
2. Resolve the API prefix from the warehouse config cache
3. Extract the namespace path from the remaining elements
4. Construct a CreateNamespaceRequest with the namespace array and properties
5. POST to `/v1/{prefix}/namespaces` endpoint
6. Return the created namespace properties

**Error Handling:**

If the namespace already exists, return error code `2` (NamespaceAlreadyExists). If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### ListNamespaces

Lists child namespaces under a given parent namespace.

The implementation:

1. Extract the warehouse from the first element of the namespace identifier
2. Resolve the API prefix from the warehouse config cache
3. Extract the parent namespace path from the remaining elements (if any)
4. GET `/v1/{prefix}/namespaces` with `parent` query parameter
5. Extract namespace names from the response

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DescribeNamespace

Retrieves properties and metadata for a namespace.

The implementation:

1. Extract the warehouse from the first element of the namespace identifier
2. Resolve the API prefix from the warehouse config cache
3. Extract the namespace path from the remaining elements
4. GET `/v1/{prefix}/namespaces/{namespace}` with URL-encoded namespace path
5. Return the namespace properties

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DropNamespace

Removes a namespace from the Iceberg catalog. Only RESTRICT mode is supported; CASCADE mode is not implemented.

The implementation:

1. Extract the warehouse from the first element of the namespace identifier
2. Resolve the API prefix from the warehouse config cache
3. Extract the namespace path from the remaining elements
4. DELETE `/v1/{prefix}/namespaces/{namespace}` with URL-encoded namespace path

**Error Handling:**

If the namespace does not exist, the operation succeeds (idempotent behavior).

If the namespace is not empty, return error code `3` (NamespaceNotEmpty).

If the server returns an error, return error code `18` (Internal).

### DeclareTable

Declares a new Lance table in the Iceberg catalog without creating the underlying data.

The implementation:

1. Extract the warehouse from the first element of the table identifier
2. Resolve the API prefix from the warehouse config cache
3. Extract the namespace path from the middle elements
4. Extract the table name from the last element
5. Construct a CreateTableRequest with:
    - `name`: the table name
    - `location`: the specified or default location (defaults to `{root}/{warehouse}/{namespace}/{table_name}`)
    - `schema`: a dummy Iceberg schema with a single nullable string column `dummy`
    - `properties`: table properties including `table_type=lance`
6. POST to `/v1/{prefix}/namespaces/{namespace}/tables`
7. Return the declared table location

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the table already exists, return error code `5` (TableAlreadyExists). If the server returns an error, return error code `18` (Internal).

### ListTables

Lists all Lance tables in a namespace.

The implementation:

1. Extract the warehouse from the first element of the namespace identifier
2. Resolve the API prefix from the warehouse config cache
3. Extract the namespace path from the remaining elements
4. GET `/v1/{prefix}/namespaces/{namespace}/tables`
5. For each table, load its metadata and filter tables where `properties.table_type=lance`
6. Extract table names from the response identifiers

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DescribeTable

Retrieves metadata for a Lance table. Only `load_detailed_metadata=false` is supported. When `load_detailed_metadata=false`, only the table location and storage_options are returned; other fields (version, table_uri, schema, stats) are null.

The implementation:

1. Extract the warehouse from the first element of the table identifier
2. Resolve the API prefix from the warehouse config cache
3. Extract the namespace path from the middle elements
4. Extract the table name from the last element
5. GET `/v1/{prefix}/namespaces/{namespace}/tables/{table}`
6. Verify the table has `table_type=lance` property
7. Return the table location and storage_options from `properties`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound).

If the table is not a Lance table, return error code `13` (InvalidInput).

If the server returns an error, return error code `18` (Internal).

### DeregisterTable

Removes a Lance table registration from the Iceberg catalog without deleting the underlying data.

The implementation:

1. Extract the warehouse from the first element of the table identifier
2. Resolve the API prefix from the warehouse config cache
3. Extract the namespace path from the middle elements
4. Extract the table name from the last element
5. DELETE `/v1/{prefix}/namespaces/{namespace}/tables/{table}?purgeRequested=false`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound). If the server returns an error, return error code `18` (Internal).
