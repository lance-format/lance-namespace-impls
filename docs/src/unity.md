# Lance Unity Namespace Implementation Spec

This document describes how the Unity Catalog implements the Lance Namespace client spec.

## Background

Unity Catalog is an open-source data catalog that provides unified governance for data and AI assets. It supports external tables and can be used to manage Lance tables through its REST API. For details on Unity Catalog, see the [Unity Catalog Documentation](https://www.unitycatalog.io/).

## Namespace Implementation Configuration Properties

The Lance Unity namespace implementation accepts the following configuration properties:

The **endpoint** property is required and specifies the Unity Catalog REST API endpoint (e.g., `http://localhost:8080`).

The **auth_token** property is optional and specifies the bearer token for authentication.

The **connect_timeout** property is optional and specifies the HTTP connection timeout in milliseconds. Default value is `10000` (10 seconds).

The **read_timeout** property is optional and specifies the HTTP read timeout in milliseconds. Default value is `300000` (5 minutes).

The **max_retries** property is optional and specifies the maximum number of retries for failed requests. Default value is `3`.

The **root** property is optional and specifies the storage root location for new tables. Default value is `/tmp/lance`.

## Object Mapping

### Namespace

The **namespace identifier** follows a hierarchical structure where the first level represents the Unity Catalog, and the second level represents a schema within that catalog. For example, `my_catalog.my_schema` refers to schema `my_schema` in catalog `my_catalog`.

The **root namespace** (empty identifier) lists all available catalogs in the Unity Catalog server.

A **child namespace** is a schema within a Unity Catalog. Unity supports a fixed 3-level hierarchy: catalog.schema.table.

**Namespace properties** are stored in the Unity schema's properties map.

### Table

A **table** is represented as a [Table](https://github.com/unitycatalog/unitycatalog/blob/main/api/all.yaml) object in Unity Catalog with `table_type` set to `EXTERNAL`.

The **table identifier** is constructed by joining catalog, schema, and table name (e.g., `catalog.schema.table`).

The **table location** is stored in the `storage_location` field of the Unity Table, pointing to the root location of the Lance table.

**Table properties** are stored in the Unity Table's `properties` map. The `columns` field stores the table schema converted from Lance's Arrow schema to Unity's column format.

## Lance Table Identification

A table in Unity Catalog is identified as a Lance table when it meets the following criteria: the `table_type` is `EXTERNAL`, and the `properties` map contains a key `table_type` with value `lance` (case insensitive). The `storage_location` must point to a valid Lance table root directory.

Note: Unity Catalog does not natively recognize the `LANCE` data source format, so `data_source_format` is set to `TEXT` as a generic format for external tables. The actual format is determined by the `table_type=lance` property.

## Basic Operations

### CreateNamespace

Creates a new schema in Unity Catalog.

The implementation:

1. Parse the namespace identifier (must be 2-level: catalog.schema)
2. Construct a CreateSchema request with name, catalog name, and properties
3. POST to `/schemas` endpoint
4. Return the created schema properties

**Error Handling:**

If the schema already exists, return error code `2` (NamespaceAlreadyExists). If the server returns an error, return error code `18` (Internal).

### ListNamespaces

Lists catalogs or schemas in the Unity Catalog.

The implementation:

1. Parse the parent namespace identifier
2. For root namespace (level 0): GET `/catalogs` to list all available catalogs
3. For catalog namespace (level 1): GET `/schemas` with catalog_name parameter to list schemas
4. Sort the results

**Error Handling:**

If the catalog does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DescribeNamespace

Retrieves properties and metadata for a schema.

The implementation:

1. Parse the namespace identifier (must be 2-level: catalog.schema)
2. GET `/schemas/{catalog}.{schema}`
3. Return the schema properties

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DropNamespace

Removes a schema from Unity Catalog. Only RESTRICT mode is supported; CASCADE mode is not implemented.

The implementation:

1. Parse the namespace identifier (must be 2-level: catalog.schema)
2. DELETE `/schemas/{catalog}.{schema}`

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound).

If the namespace is not empty, return error code `3` (NamespaceNotEmpty).

If the server returns an error, return error code `18` (Internal).

### DeclareTable

Declares a new Lance table in Unity Catalog without creating the underlying data.

The implementation:

1. Parse the table identifier (must be 3-level: catalog.schema.table)
2. Construct a CreateTable request with:
    - `name`: the table name
    - `catalog_name`: the catalog
    - `schema_name`: the schema
    - `table_type`: `EXTERNAL`
    - `data_source_format`: `TEXT`
    - `storage_location`: the specified or default location
    - `properties`: including `table_type=lance`
3. POST to `/tables` endpoint
4. Return the created table location and properties

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the table already exists, return error code `5` (TableAlreadyExists). If the server returns an error, return error code `18` (Internal).

### ListTables

Lists all Lance tables in a schema.

The implementation:

1. Parse the namespace identifier (must be 2-level: catalog.schema)
2. GET `/tables` with catalog_name and schema_name parameters
3. Filter tables where `properties.table_type=lance`
4. Sort the results

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the server returns an error, return error code `18` (Internal).

### DescribeTable

Retrieves metadata for a Lance table. Only `load_detailed_metadata=false` is supported. When `load_detailed_metadata=false`, only the table location and storage_options are returned; other fields (version, table_uri, schema, stats) are null.

The implementation:

1. Parse the table identifier (must be 3-level: catalog.schema.table)
2. GET `/tables/{catalog}.{schema}.{table}`
3. Verify the table is a Lance table (check `properties.table_type=lance`)
4. Return the table location from `storage_location` and storage_options from `properties`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound).

If the table is not a Lance table, return error code `13` (InvalidInput).

If the server returns an error, return error code `18` (Internal).

### DeregisterTable

Removes a Lance table registration from Unity Catalog without deleting the underlying data.

The implementation:

1. Parse the table identifier (must be 3-level: catalog.schema.table)
2. GET the table and verify it is a Lance table
3. DELETE `/tables/{catalog}.{schema}.{table}`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound). If the table is not a Lance table, return error code `13` (InvalidInput). If the server returns an error, return error code `18` (Internal).
