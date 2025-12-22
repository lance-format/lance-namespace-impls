# Lance Gravitino Namespace

**Apache Gravitino** is a high-performance, geo-distributed, and federated metadata lake.
It manages metadata directly in different sources, types, and regions, providing unified metadata access for data and AI assets.

## Native Lance REST Support

Starting from version 1.1.0, Apache Gravitino provides **native integration** with the Lance REST Namespace.
This allows you to use Gravitino as a Lance namespace server directly, with full support for Lance tables.

For details on configuring Gravitino as a Lance REST namespace server, see the
[Gravitino Lance REST Service Documentation](https://gravitino.apache.org/docs/1.1.0/lance-rest-service).

## Using Gravitino with Hive MetaStore

In addition to the native Lance REST support, Gravitino also provides a Hive MetaStore-compatible endpoint
through its [Apache Hive Catalog](https://gravitino.apache.org/docs/latest/apache-hive-catalog).

Simply configure your Lance Hive namespace to connect to Gravitino's Hive MetaStore endpoint.
All the features and configurations of the Lance Hive Namespace ([V2](hive2.md) or [V3](hive3.md)) apply when using Gravitino's Hive catalog.
