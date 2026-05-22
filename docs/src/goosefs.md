# Tencent GooseFS Table Master

This document describes how the [Tencent Cloud GooseFS](https://www.tencentcloud.com/document/product/1424) Table Master implements the Lance Namespace client spec.

## Background

GooseFS is a distributed caching and acceleration service developed by Tencent Cloud that provides a unified namespace for accessing data across different storage systems. The GooseFS **Table Master** is a dedicated metadata service for managing Lance tables, exposing a gRPC API that mirrors the full Lance Namespace spec (namespaces, tables, indices, tags, transactions, and table versioning).

Unlike general-purpose catalogs such as Hive Metastore or AWS Glue, GooseFS Table Master is purpose-built for Lance, so this implementation acts as a thin pass-through layer that forwards each Lance Namespace request directly to the corresponding `*PRequest` on the underlying [`goosefs-metastore-client`](https://pypi.org/project/goosefs-metastore-client/).

## Installation

The GooseFS implementation is shipped as an optional extra:

```bash
pip install 'lance-namespace-impls[goosefs]'
```

This pulls in the `goosefs-metastore-client`, `grpcio`, and `grpcio-status` dependencies.

## Namespace Implementation Configuration Properties

The Lance GooseFS namespace implementation accepts the following configuration properties:

The **uri** property is optional and specifies the GooseFS master endpoint in the form `goosefs://<host>:<port>` (e.g., `goosefs://localhost:9220`). When supplied, it takes precedence over `host` and `port`.

The **host** property is optional and specifies the GooseFS master host. Default value is `localhost`.

The **port** property is optional and specifies the GooseFS master port. Default value is `9220`.

The **timeout** property is optional and specifies the request timeout in seconds. Default value is `30`.

The **max_retries** property is optional and specifies the maximum number of retry attempts. Default value is `3`.

The **authentication_enabled** property is optional and specifies whether to enable SASL authentication. Default value is `false`. Both boolean and string (`"true"`/`"false"`) values are accepted.

The **username** property is optional and specifies the username used when authentication is enabled.

The **impersonation_user** property is optional and specifies a user to impersonate for downstream metadata operations.

The **manifest_enabled** property is optional and, when present, is forwarded as a default property to every namespace created through `CreateNamespace`. Per-request `properties` always take precedence over this default.

The **dir_listing_enabled** property is optional and, when present, is forwarded as a default property to every namespace created through `CreateNamespace`. Per-request `properties` always take precedence over this default.

## Object Mapping

### Namespace

The **root namespace** is represented by the GooseFS Table Master service itself. The Table Master does not impose an implicit prefix or default database — the namespace identifier supplied by the caller is forwarded as-is.

A **child namespace** is a database registered in the GooseFS Table Master, forming a 2-level namespace hierarchy (`database` > `table`).

The **namespace identifier** is the database name, supplied as a single-element list (e.g., `["my_database"]`).

**Namespace properties** are stored verbatim in the Table Master's namespace metadata. Optional defaults configured via `manifest_enabled` and `dir_listing_enabled` on the namespace client are merged in automatically when a namespace is created.

### Table

A **table** is represented as a Lance table object managed by the GooseFS Table Master. Because the Table Master is Lance-native, every registered table is a Lance table — there is no need for a separate `table_type=lance` marker.

The **table identifier** is supplied as a two-element list `[database, table]` (e.g., `["my_database", "my_table"]`) and is forwarded directly to the Table Master's gRPC API.

The **table location** is stored in the Table Master and points to the Lance dataset's storage root (typically a `goosefs://`, `cos://`, or `s3://` URI).

**Table properties** are stored verbatim in the Table Master's table metadata.

## Lance Table Identification

Because the GooseFS Table Master only manages Lance tables, every table returned by the Table Master is treated as a Lance table. No `table_type` filter is applied on the client side — the storage check governed by `include_declared` and `check_declared` is delegated to the Table Master itself.

## Basic Operations

All operations are implemented as thin wrappers that translate a Lance Namespace request into the corresponding `*PRequest` defined in the GooseFS Table Master gRPC schema (`grpc_files.table_master_pb2`), invoke the matching method on the shared `GoosefsMetastoreClient`, and translate the response back into the Lance Namespace response model.

### CreateNamespace

Creates a new database in the GooseFS Table Master.

The implementation:

1. Build a `CreateNamespacePRequest` from the request `id` and `mode`
2. Merge the namespace-level default properties (`manifest_enabled`, `dir_listing_enabled` configured on the client) with the request `properties`; the request value wins on conflict
3. Forward the request to `GoosefsMetastoreClient.create_namespace`
4. Honor the requested creation mode (CREATE, EXIST_OK, OVERWRITE) via the underlying gRPC API

**Error Handling:**

If the namespace already exists and mode is CREATE, the underlying gRPC error is surfaced as `NamespaceAlreadyExists`.

If the Table Master connection fails, the error is propagated to the caller.

### ListNamespaces

Lists all databases registered in the GooseFS Table Master.

The implementation:

1. Build a `ListNamespacesPRequest` from the parent namespace `id`, `page_token`, and `limit`
2. Forward the request to `GoosefsMetastoreClient.list_namespaces`
3. Return the namespace names and `next_page_token` from the gRPC response

### DescribeNamespace

Retrieves properties and metadata for a database.

The implementation:

1. Build a `DescribeNamespacePRequest` from the namespace `id`
2. Forward the request to `GoosefsMetastoreClient.describe_namespace`
3. Return the namespace `properties` from the response

**Error Handling:**

If the namespace does not exist, the error is surfaced as `NamespaceNotFound`.

### DropNamespace

Removes a database from the GooseFS Table Master.

The implementation:

1. Build a `DropNamespacePRequest` from the namespace `id`, `mode`, and `behavior`
2. Forward the request to `GoosefsMetastoreClient.drop_namespace`

**Error Handling:**

If the namespace does not exist and mode is FAIL, the error is surfaced as `NamespaceNotFound`.

If the namespace is not empty under RESTRICT behavior, the error is surfaced as `NamespaceNotEmpty`.

### NamespaceExists

Checks whether a namespace exists by forwarding a `NamespaceExistsPRequest` to the Table Master.

### CreateTable / CreateEmptyTable

Creates a new Lance table.

The implementation:

1. Build a `CreateTablePRequest` (or `CreateEmptyTablePRequest` for `CreateEmptyTable`) from the request `id`, `location`, `properties`, and any provided schema or initial data payload
2. Forward the request to the corresponding `GoosefsMetastoreClient` method
3. Return the table location, version, and properties from the response

**Error Handling:**

If the parent namespace does not exist, the error is surfaced as `NamespaceNotFound`.

If the table already exists, the error is surfaced as `TableAlreadyExists`.

### DeclareTable

Declares (registers) an existing Lance dataset in the Table Master without creating any data.

The implementation:

1. Build a `DeclareTablePRequest` from the request `id`, `location`, `properties`, and `vend_credentials`
2. Forward the request to `GoosefsMetastoreClient.declare_table`
3. Return the declared table location

**Error Handling:**

If the parent namespace does not exist, the error is surfaced as `NamespaceNotFound`.

If the table already exists, the error is surfaced as `TableAlreadyExists`.

### RegisterTable

Registers an existing dataset (similar to `DeclareTable` but with optional pre-existing metadata) by forwarding a `RegisterTablePRequest` to the Table Master.

### ListTables

Lists all Lance tables in a database.

The implementation:

1. Build a `ListTablesPRequest` from the namespace `id`, `page_token`, `limit`, and `include_declared`
2. Forward the request to `GoosefsMetastoreClient.list_tables`
3. Return the table names and `next_page_token` from the response

**Error Handling:**

If the namespace does not exist, the error is surfaced as `NamespaceNotFound`.

### DescribeTable

Retrieves metadata for a Lance table.

The implementation:

1. Build a `DescribeTablePRequest` from the request `id`, `version`, `load_detailed_metadata`, and `check_declared`
2. Forward the request to `GoosefsMetastoreClient.describe_table`
3. Return the table location, version, properties, schema, and `is_only_declared` from the response

**Error Handling:**

If the table does not exist, the error is surfaced as `TableNotFound`.

### DropTable

Removes a Lance table from the Table Master and deletes the underlying data.

The implementation:

1. Build a `DropTablePRequest` from the request `id`
2. Forward the request to `GoosefsMetastoreClient.drop_table` with delete-data semantics
3. Return the dropped table id, location, and properties

**Error Handling:**

If the table does not exist, the error is surfaced as `TableNotFound`.

### DeregisterTable

Removes a Lance table registration without deleting the underlying data.

The implementation:

1. Build a `DeregisterTablePRequest` from the request `id`
2. Forward the request to `GoosefsMetastoreClient.deregister_table`
3. Return the table id, location, and properties

**Error Handling:**

If the table does not exist, the error is surfaced as `TableNotFound`.

### TableExists

Checks whether a table exists by forwarding a `TableExistsPRequest` to the Table Master.

### RenameTable

Renames a table within its parent database by forwarding a `RenameTablePRequest`.

## Advanced Operations

In addition to the basic Lance Namespace operations, the GooseFS Table Master exposes a richer set of capabilities, all of which are forwarded transparently:

- **Data plane operations:** `InsertIntoTable`, `MergeInsertIntoTable`, `DeleteFromTable`, `UpdateTable`, `QueryTable`, `CountTableRows`
- **Schema evolution:** `AlterTableAddColumns`, `AlterTableAlterColumns`, `AlterTableDropColumns`, `UpdateTableSchemaMetadata`
- **Indexing:** `CreateTableIndex`, `CreateTableScalarIndex`, `ListTableIndices`, `DescribeTableIndexStats`, `DropTableIndex`
- **Tags & versioning:** `CreateTableTag`, `UpdateTableTag`, `DeleteTableTag`, `ListTableTags`, `GetTableTagVersion`, `CreateTableVersion`, `ListTableVersions`, `DescribeTableVersion`, `RestoreTable`, plus the batch variants `BatchCreateTableVersions` / `BatchDeleteTableVersions` / `BatchCommitTables`
- **Transactions:** `AlterTransaction`, `DescribeTransaction`
- **Query planning:** `ExplainTableQueryPlan`, `AnalyzeTableQueryPlan`, `GetTableStats`

Each operation maps one-to-one to the matching `*PRequest` / `*PResponse` pair in the Table Master gRPC schema. Refer to the [GooseFS Table Master documentation](https://www.tencentcloud.com/document/product/1424) for the authoritative request and response semantics.
