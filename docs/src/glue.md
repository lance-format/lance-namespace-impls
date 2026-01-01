# AWS Glue Data Catalog Lance Namespace Implementation Spec

This document describes how the AWS Glue Data Catalog
implements the Lance Namespace client spec.

## Background

AWS Glue Data Catalog is a fully managed metadata repository that stores structural and operational metadata for data assets. 
It is based on the Apache Hive Metastore API, but uses JSON RPC instead of Apache Thrift for request response.
It can be used as a central metadata repository for data lakes. 
For details on AWS Glue, see the [AWS Glue Data Catalog Documentation](https://docs.aws.amazon.com/glue/latest/dg/manage-catalog.html).

## Namespace Implementation Configuration Properties

The Lance Glue namespace implementation accepts the following configuration properties:

The **catalog_id** property is optional and specifies the Catalog ID of the Glue catalog to use as the starting point. When not specified, it is resolved to the caller's AWS account ID.

The **endpoint** property is optional and specifies a custom Glue service endpoint for API compatible metastores.

The **region** property is optional and specifies the AWS region for all Glue operations. When not specified, it is resolved to the default AWS region in the caller's environment.

The **access_key_id** property is optional and specifies the AWS access key ID for static credentials.

The **secret_access_key** property is optional and specifies the AWS secret access key for static credentials.

The **session_token** property is optional and specifies the AWS session token for temporary credentials.

The **assume_role_arn** property is optional and specifies the ARN of the IAM role to assume for Glue operations.

The **assume_role_region** property is optional and specifies the AWS region for the STS client when assuming a role.

The **assume_role_external_id** property is optional and specifies the external ID for cross-account role assumption. For more details, see [AWS external ID documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html).

The **assume_role_session_name** property is optional and specifies the session name for the assumed role session. For more details, see [AWS role session name documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_iam-condition-keys.html#ck_rolesessionname).

The **assume_role_timeout_sec** property is optional and specifies the duration in seconds for which the assumed role session is valid (default: 3600). At the end of the timeout, a new set of role session credentials will be fetched through the STS client.

### Authentication

The Glue namespace supports multiple authentication methods:

1. **Default AWS credential provider chain**: When no explicit credentials are provided, the client uses the default AWS credential provider chain
2. **Static credentials**: Set `access_key_id` and `secret_access_key` for basic AWS credentials
3. **Session credentials**: Additionally provide `session_token` for temporary AWS credentials
4. **Assume role credentials**: Set `assume_role_arn` to assume an IAM role. Optionally configure `assume_role_region`, `assume_role_external_id`, `assume_role_session_name`, and `assume_role_timeout_sec` to customize the role assumption behavior

## Object Mapping

### Namespace

AWS Glue Data Catalog supports a recursive catalog structure through the [GetCatalog](https://docs.aws.amazon.com/glue/latest/webapi/API_GetCatalog.html) and [GetCatalogs](https://docs.aws.amazon.com/glue/latest/webapi/API_GetCatalogs.html) APIs. 
This allows for multi-level namespace hierarchies.

The **root namespace** is represented by the default AWS Glue Data Catalog. When the `catalog_id` property is not specified or set to `None`, it is resolved to the caller's AWS account ID. Users can specify a different `catalog_id` to use another AWS account's Glue catalog as the starting point.

A **child catalog** within the root catalog forms a child namespace. The [GetCatalogs](https://docs.aws.amazon.com/glue/latest/webapi/API_GetCatalogs.html) API supports `ParentCatalogId` parameter to traverse the catalog hierarchy.

A **database** within a catalog represents the leaf namespace level. Databases are created within a specific catalog using the `CatalogId` parameter in the [CreateDatabase](https://docs.aws.amazon.com/glue/latest/webapi/API_CreateDatabase.html) API.

The **namespace identifier** follows a hierarchical pattern:
- For catalogs: the catalog name (e.g., `my_catalog`)
- For databases: the catalog chain joined with database name using the `$` delimiter (e.g., `catalog$database` or `parent_catalog$child_catalog$database`)

**Namespace properties** are stored in:
- Catalog's `Parameters` map for catalog-level namespaces
- Database's `Parameters` map for database-level namespaces

### Table

A **table** is represented as a [Table](https://docs.aws.amazon.com/glue/latest/webapi/API_Table.html) object in AWS Glue with `TableType` set to `EXTERNAL_TABLE`.

The **table identifier** is constructed by joining the full namespace path and table name with the `$` delimiter (e.g., `catalog$database$table`).

The **table location** is stored in the [`StorageDescriptor.Location`](https://docs.aws.amazon.com/glue/latest/webapi/API_StorageDescriptor.html#Glue-Type-StorageDescriptor-Location) field, pointing to the root location of the Lance table.

**Table properties** are stored in the table's [`Parameters`](https://docs.aws.amazon.com/glue/latest/webapi/API_Table.html#Glue-Type-Table-Parameters) map.

## Lance Table Identification

A table in AWS Glue is identified as a Lance table when it meets the following criteria: the `TableType` is `EXTERNAL_TABLE`, and the `Parameters` map contains a key `table_type` with value `lance` (case insensitive). The `StorageDescriptor.Location` must point to a valid Lance table root directory.

## Basic Operations

### CreateNamespace

Creates a new catalog or database in AWS Glue.

The implementation:

1. Parse the namespace identifier to determine if it is a catalog or database level
2. For catalog-level namespace:
    - Construct a [CreateCatalog](https://docs.aws.amazon.com/glue/latest/webapi/API_CreateCatalog.html) request with name and properties
    - Set the `Parameters` map with the provided namespace properties
3. For database-level namespace:
    - Verify the parent catalog exists
    - Construct a [CreateDatabase](https://docs.aws.amazon.com/glue/latest/webapi/API_CreateDatabase.html) request with database name and `CatalogId`
    - Set the `Parameters` map with the provided namespace properties
4. Handle creation mode (CREATE, EXIST_OK, OVERWRITE) appropriately

**Error Handling:**

If the namespace already exists and mode is CREATE, return error code `2` (NamespaceAlreadyExists).

If the parent catalog does not exist, return error code `1` (NamespaceNotFound).

If access is denied, return error code `16` (Forbidden).

If the Glue service is unavailable, return error code `17` (ServiceUnavailable).

### ListNamespaces

Lists catalogs or databases in AWS Glue.

The implementation:

1. Parse the parent namespace identifier
2. For root namespace (no parent):
    - Use [GetCatalogs](https://docs.aws.amazon.com/glue/latest/webapi/API_GetCatalogs.html) with `IncludeRoot=true` to list all catalogs
    - Use `ParentCatalogId` set to account ID and `Recursive=false` for direct children
3. For catalog-level namespace:
    - Use [GetDatabases](https://docs.aws.amazon.com/glue/latest/webapi/API_GetDatabases.html) with the catalog's `CatalogId`
    - Additionally use [GetCatalogs](https://docs.aws.amazon.com/glue/latest/webapi/API_GetCatalogs.html) with `ParentCatalogId` to list child catalogs
4. Sort the results and apply pagination using `NextToken`

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound).

If access is denied, return error code `16` (Forbidden).

If the Glue service is unavailable, return error code `17` (ServiceUnavailable).

### DescribeNamespace

Retrieves properties and metadata for a catalog or database.

The implementation:

1. Parse the namespace identifier to determine the level
2. For catalog-level namespace:
    - Use [GetCatalog](https://docs.aws.amazon.com/glue/latest/webapi/API_GetCatalog.html) with the catalog ID
    - Extract properties from the `Parameters` map
3. For database-level namespace:
    - Use [GetDatabase](https://docs.aws.amazon.com/glue/latest/webapi/API_GetDatabase.html) with the database name and `CatalogId`
    - Extract properties from the Database's `Parameters` map

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound).

If access is denied, return error code `16` (Forbidden).

If the Glue service is unavailable, return error code `17` (ServiceUnavailable).

### DropNamespace

Removes a catalog or database from AWS Glue. Only RESTRICT mode is supported; CASCADE mode is not implemented.

The implementation:

1. Parse the namespace identifier to determine the level
2. Check if the namespace exists (handle SKIP mode if not)
3. For catalog-level namespace:
    - Verify the catalog has no child catalogs or databases
    - Use [DeleteCatalog](https://docs.aws.amazon.com/glue/latest/webapi/API_DeleteCatalog.html) with the catalog ID
4. For database-level namespace:
    - Verify the database is empty (no tables)
    - Use [DeleteDatabase](https://docs.aws.amazon.com/glue/latest/webapi/API_DeleteDatabase.html) with the database name and `CatalogId`

**Error Handling:**

If the namespace does not exist and mode is FAIL, return error code `1` (NamespaceNotFound).

If the namespace is not empty, return error code `3` (NamespaceNotEmpty).

If access is denied, return error code `16` (Forbidden).

If the Glue service is unavailable, return error code `17` (ServiceUnavailable).

### DeclareTable

Declares a new Lance table in AWS Glue without creating the underlying data.

The implementation:

1. Parse the table identifier to extract catalog, database, and table name
2. Verify the parent namespace (database) exists using [GetDatabase](https://docs.aws.amazon.com/glue/latest/webapi/API_GetDatabase.html)
3. Construct a [CreateTable](https://docs.aws.amazon.com/glue/latest/webapi/API_CreateTable.html) request with:
    - `CatalogId`: the catalog ID from the namespace
    - `DatabaseName`: the database name
    - `TableInput.Name`: the table name
    - `TableInput.TableType`: `EXTERNAL_TABLE`
    - `TableInput.Parameters`: include `table_type=lance` and other properties
    - `TableInput.StorageDescriptor.Location`: the specified table location
4. POST the CreateTable request to Glue

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound).

If the table already exists, return error code `5` (TableAlreadyExists).

If access is denied, return error code `16` (Forbidden).

If the Glue service is unavailable, return error code `17` (ServiceUnavailable).

### ListTables

Lists all Lance tables in a database.

The implementation:

1. Parse the namespace identifier to extract catalog and database
2. Verify the namespace exists using [GetDatabase](https://docs.aws.amazon.com/glue/latest/webapi/API_GetDatabase.html)
3. Use [GetTables](https://docs.aws.amazon.com/glue/latest/webapi/API_GetTables.html) with `CatalogId` and `DatabaseName`
4. Filter tables where `Parameters.table_type=lance` (case insensitive)
5. Sort the results and apply pagination using `NextToken`

**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound).

If access is denied, return error code `16` (Forbidden).

If the Glue service is unavailable, return error code `17` (ServiceUnavailable).

### DescribeTable

Retrieves metadata for a Lance table. Only `load_detailed_metadata=false` is supported. When `load_detailed_metadata=false`, only the table location and storage_options are returned; other fields (version, table_uri, schema, stats) are null.

The implementation:

1. Parse the table identifier to extract catalog, database, and table name
2. Use [GetTable](https://docs.aws.amazon.com/glue/latest/webapi/API_GetTable.html) with `CatalogId`, `DatabaseName`, and `Name`
3. Validate that the table is a Lance table (check `Parameters.table_type=lance`)
4. Return the table location from `StorageDescriptor.Location` and storage_options from `Parameters`

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound).

If the table is not a Lance table, return error code `13` (InvalidInput).

If access is denied, return error code `16` (Forbidden).

If the Glue service is unavailable, return error code `17` (ServiceUnavailable).

### DeregisterTable

Removes a Lance table registration from AWS Glue without deleting the underlying data.

The implementation:

1. Parse the table identifier to extract catalog, database, and table name
2. Use [GetTable](https://docs.aws.amazon.com/glue/latest/webapi/API_GetTable.html) to retrieve and validate the table is a Lance table
3. Use [DeleteTable](https://docs.aws.amazon.com/glue/latest/webapi/API_DeleteTable.html) with `CatalogId`, `DatabaseName`, and `Name`
4. The underlying Lance table data at `StorageDescriptor.Location` is not deleted

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound).

If the table is not a Lance table, return error code `13` (InvalidInput).

If access is denied, return error code `16` (Forbidden).

If the Glue service is unavailable, return error code `17` (ServiceUnavailable).
