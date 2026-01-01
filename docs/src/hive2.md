# Apache Hive 2.X MetaStore Lance Namespace Implementation Spec

This document describes how the Hive 2.x MetaStore implements the Lance Namespace client spec.

## Background

Apache Hive MetaStore (HMS) is a centralized metadata repository for Apache Hive that stores schema and partition information for Hive tables. Hive 2.x uses a 2-level namespace hierarchy (database.table). For details on HMS 2.x, see the [HMS AdminManual 2.x](https://hive.apache.org/docs/latest/adminmanual-metastore-administration_27362076/).

## Namespace Implementation Configuration Properties

The Lance Hive 2.x namespace implementation accepts the following configuration properties:

The **client.pool-size** property is optional and specifies the size of the HMS client connection pool. Default value is `3`.

The **root** property is optional and specifies the storage root location of the lakehouse on Hive catalog. Default value is the current working directory.

## Object Mapping

### Namespace

The **root namespace** is represented by the HMS server itself.

A **child namespace** is a database in HMS, forming a 2-level namespace hierarchy.

The **namespace identifier** is the database name.

**Namespace properties** are stored in the HMS Database object's parameters map, with special handling for `database.description`, `database.location-uri`, `database.owner`, and `database.owner-type`.

### Table

A **table** is represented as a [Table object](https://github.com/apache/hive/blob/branch-2.3/metastore/src/gen/thrift/gen-javabean/org/apache/hadoop/hive/metastore/api/Table.java) in HMS with `tableType` set to `EXTERNAL_TABLE`.

The **table identifier** is constructed by joining database and table name with the `$` delimiter (e.g., `database$table`).

The **table location** is stored in the `location` field of the table's `storageDescriptor`, pointing to the root location of the Lance table.

**Table properties** are stored in the table's `parameters` map.

## Lance Table Identification

A table in HMS is identified as a Lance table when it meets the following criteria: the `tableType` is `EXTERNAL_TABLE`, and the `parameters` map contains a key `table_type` with value `lance` (case insensitive). The `location` in `storageDescriptor` must point to a valid Lance table root directory.

## Basic Operations

### CreateNamespace

Creates a new database in HMS.

The implementation:

1. Parse the namespace identifier (database name)
2. Create a new Database object with the specified name, location, and properties
3. Handle creation mode (CREATE, EXIST_OK, OVERWRITE) appropriately

**Error Handling:**

If the namespace already exists and mode is CREATE, return error code `2` (NamespaceAlreadyExists). If the HMS connection fails, return error code `17` (ServiceUnavailable).

### ListNamespaces

Lists all databases in HMS.

The implementation:

1. Parse the parent namespace identifier
2. For root namespace: list all databases
3. Sort the results and apply pagination

**Error Handling:**

If the HMS connection fails, return error code `17` (ServiceUnavailable).

### DescribeNamespace

Retrieves properties and metadata for a database.

The implementation:

1. Parse the namespace identifier (database name)
2. Retrieve the Database object and extract description, location, owner, and custom properties

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the HMS connection fails, return error code `17` (ServiceUnavailable).

### DropNamespace

Removes a database from HMS. Only RESTRICT mode is supported; CASCADE mode is not implemented.

The implementation:

1. Parse the namespace identifier (database name)
2. Check if the namespace exists (handle SKIP mode if not)
3. Verify the namespace is empty (no tables)
4. Drop the database from HMS

**Error Handling:**

If the namespace does not exist and mode is FAIL, return error code `1` (NamespaceNotFound).

If the namespace is not empty, return error code `3` (NamespaceNotEmpty).

If the HMS connection fails, return error code `17` (ServiceUnavailable).

### DeclareTable

Declares a new Lance table in HMS without creating the underlying data.

The implementation:

1. Parse the table identifier to extract database and table name
2. Verify the parent namespace exists
3. Create an HMS Table object with `tableType=EXTERNAL_TABLE`
4. Set the storage descriptor with the specified or default location. When location is not specified, it defaults to `{root}/{database}.db/{table}`
5. Add `table_type=lance` to the table parameters
6. Register the table in HMS

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound). If the table already exists, return error code `5` (TableAlreadyExists). If the HMS connection fails, return error code `17` (ServiceUnavailable).

### ListTables

Lists all Lance tables in a database.

The implementation:

1. Parse the namespace identifier (database name)
2. Verify the namespace exists
3. Retrieve all tables in the database
4. Filter tables where `parameters.table_type=lance`
5. Sort the results and apply pagination

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound). If the HMS connection fails, return error code `17` (ServiceUnavailable).

### DescribeTable

Retrieves metadata for a Lance table. Only `load_detailed_metadata=false` is supported. When `load_detailed_metadata=false`, only the table location is returned; other fields (version, table_uri, schema, stats) are null.

The implementation:

1. Parse the table identifier
2. Retrieve the Table object from HMS
3. Validate that it is a Lance table (check `table_type=lance`)
4. Return the table location from `storageDescriptor.location`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound).

If the table is not a Lance table, return error code `13` (InvalidInput).

If the HMS connection fails, return error code `17` (ServiceUnavailable).

### DeregisterTable

Removes a Lance table registration from HMS without deleting the underlying data.

The implementation:

1. Parse the table identifier
2. Retrieve the Table object and validate it is a Lance table
3. Drop the table from HMS with `deleteData=false`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound). If the table is not a Lance table, return error code `13` (InvalidInput). If the HMS connection fails, return error code `17` (ServiceUnavailable).
