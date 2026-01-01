# Lance Polaris Namespace Implementation Spec

This document describes how the Polaris Catalog implements the Lance Namespace client spec.

## Background

Apache Polaris is an open-source catalog implementation for Apache Iceberg that provides a REST API for managing tables and namespaces. Polaris supports the Generic Table API which allows registering non-Iceberg table formats. For details on Polaris Catalog, see the [Polaris Catalog Documentation](https://polaris.apache.org).

**Note:** The Generic Table API is available in Polaris 1.2.0-incubating and later versions. Ensure your Polaris deployment is running a compatible version.

## Namespace Implementation Configuration Properties

The Lance Polaris namespace implementation accepts the following configuration properties:

The **endpoint** property is required and specifies the Polaris server endpoint URL (e.g., `http://localhost:8181`). Must start with `http://` or `https://`.

The **auth_token** property is optional and specifies the bearer token for authentication.

The **connect_timeout** property is optional and specifies the connection timeout in milliseconds. Default value is `10000` (10 seconds).

The **read_timeout** property is optional and specifies the read timeout in milliseconds. Default value is `30000` (30 seconds).

The **max_retries** property is optional and specifies the maximum number of retries for failed requests. Default value is `3`.

## Object Mapping

### Catalog

The **catalog** is the first element of any namespace or table identifier. It determines which Polaris catalog the operations target.

### Namespace

The **root namespace** is represented by specifying only the catalog in the identifier (e.g., `["my-catalog"]`). This lists all top-level namespaces under that catalog.

A **child namespace** is a nested namespace in Polaris. Polaris supports arbitrary nesting depth. The **namespace identifier** format is `[catalog, namespace1, namespace2, ...]` (e.g., `["my-catalog", "schema", "subschema"]`).

For API calls, namespace levels (excluding the catalog) are joined with the `.` delimiter. The catalog is used in the API path as `/api/catalog/v1/{catalog}/namespaces`.

**Namespace properties** are stored in the namespace's properties map, returned by the Polaris namespace API.

### Table

A **table** is represented as a [Generic Table](https://github.com/polaris-catalog/polaris/blob/main/spec/polaris-catalog-apis/generic-tables-api.yaml) object in Polaris with `format` set to `lance`.

The **table identifier** format is `[catalog, namespace1, namespace2, ..., table_name]` (e.g., `["my-catalog", "schema", "my_table"]`).

The **table location** is stored in the `base-location` field of the Generic Table, pointing to the root location of the Lance table.

**Table properties** are stored in the Generic Table's `properties` map. An optional `doc` field can store a table description.

## Lance Table Identification

A table in Polaris is identified as a Lance table when it is a Generic Table with `format` set to `lance`. The `base-location` must point to a valid Lance table root directory. The table `properties` should contain `table_type=lance` for consistency with other catalog implementations.

## Basic Operations

### CreateNamespace

Creates a new namespace in Polaris.

The implementation:

1. Parse the namespace identifier to extract the catalog (first level) and namespace levels
2. Validate that at least 2 levels are provided (catalog + namespace)
3. Construct a CreateNamespaceRequest with the namespace array and properties
4. POST to `/api/catalog/v1/{catalog}/namespaces` endpoint
5. Return the created namespace properties

**Error Handling:**

If the namespace already exists, return error code `2` (NamespaceAlreadyExists). If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### ListNamespaces

Lists child namespaces under a given parent namespace.

The implementation:

1. Parse the parent namespace identifier to extract the catalog (first level)
2. Validate that at least 1 level (catalog) is provided
3. For catalog-level listing: GET `/api/catalog/v1/{catalog}/namespaces`
4. For nested namespace listing: GET `/api/catalog/v1/{catalog}/namespaces/{parent}/namespaces`
5. Convert the response namespace arrays to dot-separated strings, prefixing with the catalog name

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DescribeNamespace

Retrieves properties and metadata for a namespace.

The implementation:

1. Parse the namespace identifier to extract the catalog (first level) and namespace path
2. Validate that at least 2 levels are provided (catalog + namespace)
3. GET `/api/catalog/v1/{catalog}/namespaces/{namespace}` with URL-encoded namespace path
4. Return the namespace properties

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DropNamespace

Removes a namespace from Polaris. Only RESTRICT mode is supported; CASCADE mode is not implemented.

The implementation:

1. Parse the namespace identifier to extract the catalog (first level) and namespace path
2. Validate that at least 2 levels are provided (catalog + namespace)
3. DELETE `/api/catalog/v1/{catalog}/namespaces/{namespace}` with URL-encoded namespace path

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound).

If the namespace is not empty, return error code `3` (NamespaceNotEmpty).

If the server returns an error, return error code `18` (Internal).

### DeclareTable

Declares a new Lance table in Polaris without creating the underlying data.

The implementation:

1. Parse the table identifier to extract catalog (first level), namespace (middle levels), and table name (last level)
2. Validate that at least 3 levels are provided (catalog + namespace + table)
3. Construct a CreateGenericTableRequest with:
    - `name`: the table name
    - `format`: `lance`
    - `base-location`: the specified location
    - `doc`: optional description from properties
    - `properties`: table properties including `table_type=lance`
4. POST to `/api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/generic-tables`
5. Return the created table location and properties

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the table already exists, return error code `5` (TableAlreadyExists). If the server returns an error, return error code `18` (Internal).

### ListTables

Lists all Lance tables in a namespace.

The implementation:

1. Parse the namespace identifier to extract the catalog (first level) and namespace path
2. Validate that at least 2 levels are provided (catalog + namespace)
3. GET `/api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/generic-tables`
4. Extract table names from the response identifiers

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DescribeTable

Retrieves metadata for a Lance table. Only `load_detailed_metadata=false` is supported. When `load_detailed_metadata=false`, only the table location and storage_options are returned; other fields (version, table_uri, schema, stats) are null.

The implementation:

1. Parse the table identifier to extract catalog (first level), namespace (middle levels), and table name (last level)
2. Validate that at least 3 levels are provided (catalog + namespace + table)
3. GET `/api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/generic-tables/{table}`
4. Verify the table format is `lance`
5. Return the table location from `base-location` and storage_options from `properties`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound).

If the table format is not `lance`, return error code `13` (InvalidInput).

If the server returns an error, return error code `18` (Internal).

### DeregisterTable

Removes a Lance table registration from Polaris without deleting the underlying data.

The implementation:

1. Parse the table identifier to extract catalog (first level), namespace (middle levels), and table name (last level)
2. Validate that at least 3 levels are provided (catalog + namespace + table)
3. DELETE `/api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/generic-tables/{table}`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound). If the server returns an error, return error code `18` (Internal).
