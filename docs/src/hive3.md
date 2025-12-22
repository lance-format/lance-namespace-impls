# Lance Hive 3.x Namespace Implementation Spec

This document describes how the Hive 3.x MetaStore implements the Lance Namespace client spec.

## Background

Apache Hive MetaStore (HMS) is a centralized metadata repository for Apache Hive that stores schema and partition information for Hive tables. Hive 3.x introduces a 3-level namespace hierarchy (catalog.database.table) with an additional catalog level. For details on HMS 3.x, see the [HMS AdminManual 3.x](https://hive.apache.org/docs/latest/adminmanual-metastore-3-0-administration_75978150/).

## Namespace Implementation Configuration Properties

The Lance Hive 3.x namespace implementation accepts the following configuration properties:

The **client.pool-size** property is optional and specifies the size of the HMS client connection pool. Default value is `3`.

The **root** property is optional and specifies the storage root location of the lakehouse on Hive catalog. Default value is the current working directory.

The **storage.*** prefix properties are optional and specify additional storage configurations to access tables (e.g., `storage.region=us-west-2`).

## Object Mapping

### Namespace

The **root namespace** is represented by the HMS server itself.

The first-level **child namespace** is a catalog in HMS, and the second-level child namespace is a database within that catalog, forming a 3-level namespace hierarchy.

The **namespace identifier** is constructed by joining namespace levels with the `$` delimiter (e.g., `catalog$database`).

**Namespace properties** are stored in the HMS Database object's parameters map, with special handling for `database.description`, `database.location-uri`, `database.owner`, and `database.owner-type`. Catalog properties are stored in the Catalog object.

### Table

A **table** is represented as a [Table object](https://github.com/apache/hive/blob/branch-4.0/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift#L631) in HMS with `tableType` set to `EXTERNAL_TABLE`.

The **table identifier** is constructed by joining catalog, database, and table name with the `$` delimiter (e.g., `catalog$database$table`).

The **table location** is stored in the [`location`](https://github.com/apache/hive/blob/branch-4.0/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift#L467) field of the table's [`storageDescriptor`](https://github.com/apache/hive/blob/branch-4.0/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift#L638), pointing to the root location of the Lance table.

**Table properties** are stored in the table's [`parameters`](https://github.com/apache/hive/blob/branch-4.0/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift#L640) map.

## Lance Table Identification

A table in HMS is identified as a Lance table when it meets the following criteria: the `tableType` is `EXTERNAL_TABLE`, and the `parameters` map contains a key `table_type` with value `lance` (case insensitive). The `location` in `storageDescriptor` must point to a valid Lance table root directory.

## Basic Operations

### CreateNamespace

Creates a new namespace (catalog or database) in HMS.

The implementation:

1. Parse the namespace identifier to determine the level (catalog or database)
2. For catalog creation: create a new Catalog object with the specified name and location
3. For database creation: verify the parent catalog exists, then create a new Database object with the specified name, location, and properties
4. Handle creation mode (CREATE, EXIST_OK, OVERWRITE) appropriately

**Error Handling:**

If the namespace already exists and mode is CREATE, return error code `2` (NamespaceAlreadyExists). If the parent catalog does not exist when creating a database, return error code `1` (NamespaceNotFound). If the HMS connection fails, return error code `17` (ServiceUnavailable).

### ListNamespaces

Lists child namespaces under a given parent namespace.

The implementation:

1. Parse the parent namespace identifier
2. For root namespace: list all catalogs
3. For catalog namespace: list all databases in that catalog
4. Sort the results and apply pagination

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the HMS connection fails, return error code `17` (ServiceUnavailable).

### DescribeNamespace

Retrieves properties and metadata for a namespace.

The implementation:

1. Parse the namespace identifier
2. For catalog: retrieve the Catalog object and extract description and location
3. For database: retrieve the Database object and extract description, location, owner, and custom properties

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the HMS connection fails, return error code `17` (ServiceUnavailable).

### DropNamespace

Removes a namespace from HMS.

The implementation:

1. Parse the namespace identifier
2. Check if the namespace exists (handle SKIP mode if not)
3. For RESTRICT behavior: verify the namespace is empty (no child namespaces or tables)
4. For CASCADE behavior: recursively drop all child objects first
5. Drop the catalog or database from HMS

**Error Handling:**

If the namespace does not exist and mode is FAIL, return error code `1` (NamespaceNotFound). If the namespace is not empty and behavior is RESTRICT, return error code `3` (NamespaceNotEmpty). If the HMS connection fails, return error code `17` (ServiceUnavailable).

### DeclareTable

Declares a new Lance table in HMS without creating the underlying data.

The implementation:

1. Parse the table identifier to extract catalog, database, and table name
2. Verify the parent namespace exists
3. Create an HMS Table object with `tableType=EXTERNAL_TABLE`
4. Set the storage descriptor with the specified or default location
5. Add `table_type=lance` to the table parameters
6. Register the table in HMS

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the table already exists, return error code `5` (TableAlreadyExists). If the HMS connection fails, return error code `17` (ServiceUnavailable).

### ListTables

Lists all Lance tables in a namespace.

The implementation:

1. Parse the namespace identifier (catalog$database)
2. Verify the namespace exists
3. Retrieve all tables in the database
4. Filter tables where `parameters.table_type=lance`
5. Sort the results and apply pagination

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the HMS connection fails, return error code `17` (ServiceUnavailable).

### DescribeTable

Retrieves metadata for a Lance table.

The implementation:

1. Parse the table identifier
2. Retrieve the Table object from HMS
3. Validate that it is a Lance table (check `table_type=lance`)
4. Return the table location and properties

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound). If the table is not a Lance table, return error code `13` (InvalidInput). If the HMS connection fails, return error code `17` (ServiceUnavailable).

### DeregisterTable

Removes a Lance table registration from HMS without deleting the underlying data.

The implementation:

1. Parse the table identifier
2. Retrieve the Table object and validate it is a Lance table
3. Drop the table from HMS with `deleteData=false`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound). If the table is not a Lance table, return error code `13` (InvalidInput). If the HMS connection fails, return error code `17` (ServiceUnavailable).
