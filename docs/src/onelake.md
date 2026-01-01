# Azure OneLake Table API

**Microsoft OneLake** is a unified, logical data lake for Microsoft Fabric that provides a single SaaS experience and a tenant-wide store for data that serves both professional and citizen data integration needs.

Lance at this moment does not provide a dedicated OneLake namespace implementation.
However, OneLake provides a Unity Catalog-compatible endpoint through its Table APIs,
which allows you to use OneLake through the **Lance Unity Namespace**.

## Using OneLake with Lance

OneLake provides two catalog-compatible endpoints through its Table APIs, allowing you to use OneLake with Lance via either the Unity Namespace or the Iceberg REST Catalog Namespace.

### Option 1: Unity Catalog Endpoint

To use Microsoft OneLake with Lance via Unity Catalog, leverage OneLake's [Delta Lake REST API](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview),
which exposes a Unity Catalog-compatible interface.

Configure your Lance Unity namespace to connect to OneLake's Unity Catalog endpoint at:

```
https://onelake.table.fabric.microsoft.com/delta
```

All the features and configurations of the [Lance Unity Namespace](unity.md) apply when using OneLake.

### Option 2: Iceberg REST Catalog Endpoint

OneLake also provides an [Iceberg REST Catalog API](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/iceberg-table-apis-overview),
which allows you to use OneLake through the **Lance Iceberg REST Catalog Namespace**.

Configure your Lance Iceberg REST Catalog namespace to connect to OneLake's Iceberg endpoint at:

```
https://onelake.table.fabric.microsoft.com/iceberg
```

All the features and configurations of the [Lance Iceberg REST Catalog Namespace](iceberg.md) apply when using OneLake.
