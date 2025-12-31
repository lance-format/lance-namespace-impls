# Lance Iceberg REST Catalog Implementation Spec

This document describes how the Apache Iceberg REST Catalog implements the Lance Namespace client spec.

## Background

Apache Iceberg REST Catalog is a standardized REST API for interacting with Iceberg catalogs. It provides a vendor-neutral interface for managing tables and namespaces across different catalog backends. When registering a Lance table, the implementation creates a companion Iceberg table with a dummy schema at the same location, using table properties to identify it as a Lance table. For details on the Iceberg REST Catalog, see the [Iceberg REST Catalog Specification](https://iceberg.apache.org/docs/latest/rest-spec/).

## Namespace Implementation Configuration Properties

The Lance Iceberg REST Catalog namespace implementation accepts the following configuration properties:

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `endpoint` | Yes | - | Iceberg REST Catalog server endpoint URL (e.g., `http://localhost:8181`). Must start with `http://` or `https://`. |
| `warehouse` | No | - | Warehouse identifier. Some Iceberg REST implementations require this. The warehouse name is resolved to an API prefix via the `/v1/config` endpoint. |
| `auth_token` | No | - | Bearer token for authentication. |
| `credential` | No | - | OAuth2 client credential in `client_id:client_secret` format for client credentials authentication flow. |
| `connect_timeout` | No | `10000` | Connection timeout in milliseconds. |
| `read_timeout` | No | `30000` | Read timeout in milliseconds. |
| `max_retries` | No | `3` | Maximum number of retries for failed requests. |
| `root` | No | current working directory | Default storage root location for tables. |

## Object Mapping

### Namespace

The **root namespace** is represented by the Iceberg catalog root, accessed via the `/namespaces` endpoint.

A **child namespace** is a nested namespace in Iceberg. Iceberg supports arbitrary nesting depth using an array of strings (e.g., `["level1", "level2", "level3"]`).

The **namespace identifier** is constructed by joining namespace levels with the `\x1F` (unit separator) character for API calls. In user-facing contexts, a `.` delimiter is used.

**Namespace properties** are stored in the namespace's properties map, returned by the Iceberg namespace API.

### Table

A **table** is represented as a regular Iceberg table with a dummy schema. The dummy schema contains a single nullable string column named `dummy`. This approach ensures compatibility with the Iceberg REST Catalog API while storing the actual Lance table at the same location.

The **table identifier** is constructed by joining the namespace path and table name.

The **table location** is stored in the `location` field of the Iceberg table metadata, pointing to the root location of the Lance table.

**Table properties** are stored in the Iceberg table's `properties` map.

## Lance Table Identification

A table in Iceberg REST Catalog is identified as a Lance table when the `properties` map contains a key `table_type` with value `lance` (case insensitive). The `location` must point to a valid Lance table root directory. The Iceberg table itself serves as a metadata wrapper, with the actual data stored in Lance format.

## Basic Operations

### CreateNamespace

Creates a new namespace in the Iceberg catalog.

The implementation:

1. Parse the namespace identifier to get the namespace array
2. Construct a CreateNamespaceRequest with the namespace array and properties
3. POST to `/v1/{prefix}/namespaces` endpoint
4. Return the created namespace properties

**Error Handling:**

If the namespace already exists, return error code `2` (NamespaceAlreadyExists). If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### ListNamespaces

Lists child namespaces under a given parent namespace.

The implementation:

1. Parse the parent namespace identifier
2. GET `/v1/{prefix}/namespaces` with `parent` query parameter
3. Extract namespace names from the response

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DescribeNamespace

Retrieves properties and metadata for a namespace.

The implementation:

1. Parse the namespace identifier
2. GET `/v1/{prefix}/namespaces/{namespace}` with URL-encoded namespace path
3. Return the namespace properties

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DropNamespace

Removes a namespace from the Iceberg catalog. Only RESTRICT mode is supported; CASCADE mode is not implemented.

The implementation:

1. Parse the namespace identifier
2. DELETE `/v1/{prefix}/namespaces/{namespace}` with URL-encoded namespace path

**Error Handling:**

If the namespace does not exist, the operation succeeds (idempotent behavior).

If the namespace is not empty, return error code `3` (NamespaceNotEmpty).

If the server returns an error, return error code `18` (Internal).

### DeclareTable

Declares a new Lance table in the Iceberg catalog without creating the underlying data.

The implementation:

1. Parse the table identifier to extract namespace and table name
2. Construct a CreateTableRequest with:
    - `name`: the table name
    - `location`: the specified or default location (defaults to `{root}/{prefix}/{namespace}/{table_name}`)
    - `schema`: a dummy Iceberg schema with a single nullable string column `dummy`
    - `properties`: table properties including `table_type=lance`
3. POST to `/v1/{prefix}/namespaces/{namespace}/tables`
4. Return the declared table location

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the table already exists, return error code `5` (TableAlreadyExists). If the server returns an error, return error code `18` (Internal).

### ListTables

Lists all Lance tables in a namespace.

The implementation:

1. Parse the namespace identifier
2. GET `/v1/{prefix}/namespaces/{namespace}/tables`
3. For each table, load its metadata and filter tables where `properties.table_type=lance`
4. Extract table names from the response identifiers

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DescribeTable

Retrieves metadata for a Lance table. Only `load_detailed_metadata=false` is supported. When `load_detailed_metadata=false`, only the table location and storage_options are returned; other fields (version, table_uri, schema, stats) are null.

The implementation:

1. Parse the table identifier to extract namespace and table name
2. GET `/v1/{prefix}/namespaces/{namespace}/tables/{table}`
3. Verify the table has `table_type=lance` property
4. Return the table location and storage_options from `properties`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound).

If the table is not a Lance table, return error code `13` (InvalidInput).

If the server returns an error, return error code `18` (Internal).

### DeregisterTable

Removes a Lance table registration from the Iceberg catalog without deleting the underlying data.

The implementation:

1. Parse the table identifier to extract namespace and table name
2. DELETE `/v1/{prefix}/namespaces/{namespace}/tables/{table}?purgeRequested=false`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound). If the server returns an error, return error code `18` (Internal).
