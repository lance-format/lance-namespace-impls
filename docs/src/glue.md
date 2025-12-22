# Lance Glue Namespace Implementation Spec

This document describes how the AWS Glue Data Catalog implements the Lance Namespace client spec.

## Background

AWS Glue Data Catalog is a fully managed metadata repository that stores structural and operational metadata for data assets. It is compatible with the Apache Hive Metastore API and can be used as a central metadata repository for data lakes. For details on AWS Glue, see the [AWS Glue Data Catalog Documentation](https://docs.aws.amazon.com/glue/).

## Namespace Implementation Configuration Properties

The Lance Glue namespace implementation accepts the following configuration properties:

The **catalog_id** property is optional and specifies the Catalog ID of the Glue catalog (defaults to AWS account ID).

The **endpoint** property is optional and specifies a custom Glue service endpoint for API compatible metastores.

The **region** property is optional and specifies the AWS region for all Glue operations.

The **access_key_id** property is optional and specifies the AWS access key ID for static credentials.

The **secret_access_key** property is optional and specifies the AWS secret access key for static credentials.

The **session_token** property is optional and specifies the AWS session token for temporary credentials.

The **root** property is optional and specifies the storage root location of the lakehouse on Glue catalog. Default value is the current working directory.

The **storage.*** prefix properties are optional and specify additional storage configurations to access tables (e.g., `storage.region=us-west-2`).

### Authentication

The Glue namespace supports multiple authentication methods:

1. **Default AWS credential provider chain**: When no explicit credentials are provided, the client uses the default AWS credential provider chain
2. **Static credentials**: Set `access_key_id` and `secret_access_key` for basic AWS credentials
3. **Session credentials**: Additionally provide `session_token` for temporary AWS credentials

## Object Mapping

### Namespace

The **root namespace** is represented by the AWS Glue Data Catalog itself.

A **child namespace** is a database in Glue, forming a 2-level namespace hierarchy.

The **namespace identifier** is the database name.

**Namespace properties** are stored in the Glue Database object's parameters map.

### Table

A **table** is represented as a [Table](https://docs.aws.amazon.com/glue/latest/webapi/API_Table.html) object in AWS Glue with `TableType` set to `EXTERNAL_TABLE`.

The **table identifier** is constructed by joining database and table name with the `$` delimiter (e.g., `database$table`).

The **table location** is stored in the [`StorageDescriptor.Location`](https://docs.aws.amazon.com/glue/latest/webapi/API_StorageDescriptor.html#Glue-Type-StorageDescriptor-Location) field, pointing to the root location of the Lance table.

**Table properties** are stored in the table's [`Parameters`](https://docs.aws.amazon.com/glue/latest/webapi/API_Table.html#Glue-Type-Table-Parameters) map.

## Lance Table Identification

A table in AWS Glue is identified as a Lance table when it meets the following criteria: the `TableType` is `EXTERNAL_TABLE`, and the `Parameters` map contains a key `table_type` with value `lance` (case insensitive). The `StorageDescriptor.Location` must point to a valid Lance table root directory.

## Optimistic Concurrency Control

Updates to Lance tables in AWS Glue should use the `VersionId` for conditional updates through the [UpdateTable](https://docs.aws.amazon.com/glue/latest/webapi/API_UpdateTable.html) API. If the `VersionId` does not match the expected version, the update fails to prevent concurrent modification conflicts.
