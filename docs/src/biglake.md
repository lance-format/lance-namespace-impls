# Lance BigLake Namespace

**Google BigLake Metastore** is a fully managed, unified metastore service for data lakes on Google Cloud.

To use Google BigLake Metastore with Lance, you can leverage BigLake's [Iceberg REST Catalog](https://docs.cloud.google.com/biglake/docs/blms-rest-catalog),
which exposes an Apache Iceberg REST Catalog-compatible interface.

## Configuration

Configure your Lance Iceberg namespace to connect to the BigLake Metastore endpoint:

- **endpoint**: `https://biglake.googleapis.com/iceberg/v1/restcatalog`
- **warehouse**: Your BigLake catalog name in the format `projects/{project}/locations/{location}/catalogs/{catalog}`
- **auth_token**: A valid Google Cloud OAuth2 access token

All the features and configurations of the [Lance Iceberg REST Catalog Namespace](iceberg.md) apply when using BigLake Metastore.
