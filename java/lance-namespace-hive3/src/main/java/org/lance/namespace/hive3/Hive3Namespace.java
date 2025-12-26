/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.namespace.hive3;

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.ServiceUnavailableException;
import org.lance.namespace.errors.TableAlreadyExistsException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.CreateEmptyTableResponse;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.TableExistsRequest;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Hive3Namespace implements LanceNamespace {
  private static final Logger LOG = LoggerFactory.getLogger(Hive3Namespace.class);

  private Hive3ClientPool clientPool;
  private Configuration hadoopConf;
  private BufferAllocator allocator;
  private Hive3NamespaceConfig config;

  public Hive3Namespace() {}

  /** Sets the Hadoop configuration. Must be called before initialize(). */
  public void setHadoopConf(Configuration conf) {
    this.hadoopConf = conf;
  }

  @Override
  public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
    this.allocator = allocator;
    if (hadoopConf == null) {
      LOG.warn("Hadoop configuration not set, using the default configuration.");
      hadoopConf = new Configuration();
    }

    this.config = new Hive3NamespaceConfig(configProperties);
    this.clientPool = new Hive3ClientPool(config.getClientPoolSize(), hadoopConf);
  }

  @Override
  public String namespaceId() {
    String metastoreUri =
        hadoopConf != null
            ? hadoopConf.get(HiveConf.ConfVars.METASTOREURIS.varname, "default")
            : "default";
    return String.format("Hive3Namespace { metastoreUri: \"%s\" }", metastoreUri);
  }

  @Override
  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(nsId.levels() <= 2, "Expect a 2-level namespace but get %s", nsId);

    List<String> namespaces = doListNamespaces(nsId);

    Collections.sort(namespaces);
    PageUtil.Page page =
        PageUtil.splitPage(
            namespaces, request.getPageToken(), PageUtil.normalizePageSize(request.getLimit()));

    ListNamespacesResponse response = new ListNamespacesResponse();
    response.setNamespaces(Sets.newHashSet(page.items()));
    response.setPageToken(page.nextPageToken());
    return response;
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    ObjectIdentifier id = ObjectIdentifier.of(request.getId());
    String mode = request.getMode() != null ? request.getMode().toLowerCase() : "create";
    Map<String, String> properties = request.getProperties();

    ValidationUtil.checkArgument(
        !id.isRoot() && id.levels() <= 2, "Expect a 3-level namespace but get %s", id);

    doCreateNamespace(id, mode, properties);

    CreateNamespaceResponse response = new CreateNamespaceResponse();
    response.setProperties(properties);
    return response;
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
    ObjectIdentifier id = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        !id.isRoot() && id.levels() <= 2, "Expect a 2-level namespace but get %s", id);

    DescribeNamespaceResponse response = new DescribeNamespaceResponse();
    Map<String, String> properties = new HashMap<>();

    if (id.levels() == 1) {
      String catalog = id.levelAtListPos(0).toLowerCase();
      Catalog catalogObj = Hive3Util.getCatalogOrNull(clientPool, catalog);

      if (catalogObj == null) {
        throw new NamespaceNotFoundException(
            String.format("Namespace does not exist: %s", id.stringStyleId()));
      }

      if (catalogObj.getDescription() != null) {
        properties.put("description", catalogObj.getDescription());
      }
      if (catalogObj.getLocationUri() != null) {
        properties.put("catalog.location.uri", catalogObj.getLocationUri());
      }
    } else {
      String catalog = id.levelAtListPos(0).toLowerCase();
      String db = id.levelAtListPos(1).toLowerCase();
      Database database = Hive3Util.getDatabaseOrNull(clientPool, catalog, db);

      if (database == null) {
        throw new NamespaceNotFoundException(
            String.format("Namespace does not exist: %s", id.stringStyleId()));
      }

      if (database.getDescription() != null) {
        properties.put(Hive3NamespaceConfig.DATABASE_DESCRIPTION, database.getDescription());
      }
      if (database.getLocationUri() != null) {
        properties.put(Hive3NamespaceConfig.DATABASE_LOCATION_URI, database.getLocationUri());
      }
      if (database.getOwnerName() != null) {
        properties.put(Hive3NamespaceConfig.DATABASE_OWNER, database.getOwnerName());
      }
      if (database.getOwnerType() != null) {
        properties.put(Hive3NamespaceConfig.DATABASE_OWNER_TYPE, database.getOwnerType().name());
      }

      if (database.getParameters() != null) {
        properties.putAll(database.getParameters());
      }
    }

    response.setProperties(properties);
    return response;
  }

  @Override
  public void namespaceExists(NamespaceExistsRequest request) {
    ObjectIdentifier id = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        !id.isRoot() && id.levels() <= 2, "Expect a 2-level namespace but get %s", id);

    if (id.levels() == 1) {
      String catalog = id.levelAtListPos(0).toLowerCase();
      Catalog catalogObj = Hive3Util.getCatalogOrNull(clientPool, catalog);

      if (catalogObj == null) {
        throw new NamespaceNotFoundException(
            String.format("Namespace does not exist: %s", id.stringStyleId()));
      }
    } else {
      String catalog = id.levelAtListPos(0).toLowerCase();
      String db = id.levelAtListPos(1).toLowerCase();
      Database database = Hive3Util.getDatabaseOrNull(clientPool, catalog, db);

      if (database == null) {
        throw new NamespaceNotFoundException(
            String.format("Namespace does not exist: %s", id.stringStyleId()));
      }
    }
  }

  @Override
  public DropNamespaceResponse dropNamespace(DropNamespaceRequest request) {
    ObjectIdentifier id = ObjectIdentifier.of(request.getId());
    String mode = request.getMode() != null ? request.getMode().toLowerCase() : "fail";

    ValidationUtil.checkArgument(
        !id.isRoot() && id.levels() <= 2, "Expect a 2-level namespace but get %s", id);

    Map<String, String> properties = doDropNamespace(id, mode);

    DropNamespaceResponse response = new DropNamespaceResponse();
    response.setProperties(properties);
    return response;
  }

  @Override
  public void tableExists(TableExistsRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        tableId.levels() == 3, "Expect 3-level table identifier but get %s", tableId);

    String catalog = tableId.levelAtListPos(0).toLowerCase();
    String db = tableId.levelAtListPos(1).toLowerCase();
    String table = tableId.levelAtListPos(2).toLowerCase();

    Optional<Table> hmsTable = Hive3Util.getTable(clientPool, catalog, db, table);

    if (!hmsTable.isPresent()) {
      throw new TableNotFoundException(
          String.format("Table does not exist: %s", tableId.stringStyleId()));
    }

    Hive3Util.validateLanceTable(hmsTable.get());
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        !nsId.isRoot() && nsId.levels() == 2, "Expect a 2-level namespace but get %s", nsId);

    String catalog = nsId.levelAtListPos(0).toLowerCase();
    String db = nsId.levelAtListPos(1).toLowerCase();
    List<String> tables = doListTables(catalog, db);

    Collections.sort(tables);
    PageUtil.Page page =
        PageUtil.splitPage(
            tables, request.getPageToken(), PageUtil.normalizePageSize(request.getLimit()));

    ListTablesResponse response = new ListTablesResponse();
    response.setTables(Sets.newHashSet(page.items()));
    response.setPageToken(page.nextPageToken());
    return response;
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        tableId.levels() == 3, "Expect 3-level table identifier but get %s", tableId);

    Optional<String> location = doDescribeTable(tableId);

    if (!location.isPresent()) {
      throw new TableNotFoundException(
          String.format("Table does not exist: %s", tableId.stringStyleId()));
    }

    DescribeTableResponse response = new DescribeTableResponse();
    response.setLocation(location.get());
    return response;
  }

  // Removed: createTable(CreateTableRequest, byte[]) - using default implementation from interface

  @Override
  public CreateEmptyTableResponse createEmptyTable(CreateEmptyTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        tableId.levels() == 3, "Expect 3-level table identifier but get %s", tableId);

    String location = request.getLocation();
    if (location == null || location.isEmpty()) {
      location =
          getDefaultTableLocation(
              tableId.levelAtListPos(0), tableId.levelAtListPos(1), tableId.levelAtListPos(2));
    }

    // Create table in metastore without data (pass null for requestData and properties)
    doCreateTable(tableId, null, location, null, null);

    CreateEmptyTableResponse response = new CreateEmptyTableResponse();
    response.setLocation(location);
    return response;
  }

  // Removed: dropTable(DropTableRequest) - using default implementation from interface

  public void setConf(Configuration conf) {
    this.hadoopConf = conf;
  }

  protected List<String> doListNamespaces(ObjectIdentifier parent) {
    try {
      if (parent.isRoot()) {
        return clientPool.run(IMetaStoreClient::getCatalogs);
      } else if (parent.levels() == 1) {
        return clientPool.run(client -> client.getAllDatabases(parent.levelAtListPos(0)));
      } else {
        return Lists.newArrayList();
      }
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new ServiceUnavailableException("Failed operation: " + errorMessage);
    }
  }

  protected void doCreateNamespace(
      ObjectIdentifier id, String mode, Map<String, String> properties) {

    try {
      if (id.levels() == 1) {
        String name = id.levelAtListPos(0).toLowerCase();
        createCatalog(name, mode, properties);
      } else {
        String catalog = id.levelAtListPos(0).toLowerCase();
        String db = id.levelAtListPos(1).toLowerCase();
        createDatabase(catalog, db, mode, properties);
      }
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new ServiceUnavailableException("Failed operation: " + errorMessage);
    }
  }

  private void createCatalog(String catalogName, String mode, Map<String, String> properties)
      throws TException, InterruptedException {

    Catalog existingCatalog = Hive3Util.getCatalogOrNull(clientPool, catalogName);
    if (existingCatalog != null) {
      if ("create".equals(mode)) {
        throw new NamespaceAlreadyExistsException(
            String.format("Catalog %s already exists", catalogName));
      } else if ("exist_ok".equals(mode) || "existok".equals(mode)) {
        return;
      } else if ("overwrite".equals(mode)) {
        clientPool.run(
            client -> {
              client.dropCatalog(catalogName);
              return null;
            });
      }
    }

    Catalog catalog = new Catalog();
    catalog.setName(catalogName);

    String locationUri = properties != null ? properties.get("catalog.location.uri") : null;
    if (locationUri == null) {
      locationUri =
          hadoopConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname) + "/" + catalogName;
    }
    catalog.setLocationUri(locationUri);

    String description = properties != null ? properties.get("description") : null;
    if (description != null) {
      catalog.setDescription(description);
    } else {
      catalog.setDescription("Lance catalog: " + catalogName);
    }

    clientPool.run(
        client -> {
          client.createCatalog(catalog);
          return null;
        });
  }

  private void createDatabase(
      String catalogName, String dbName, String mode, Map<String, String> properties)
      throws TException, InterruptedException {
    Catalog catalog = Hive3Util.getCatalogOrThrowNotFoundException(clientPool, catalogName);

    Database oldDb = Hive3Util.getDatabaseOrNull(clientPool, catalogName, dbName);
    if (oldDb != null) {
      if ("create".equals(mode)) {
        throw new NamespaceAlreadyExistsException(
            String.format("Database %s.%s already exist", catalogName, dbName));
      } else if ("exist_ok".equals(mode) || "existok".equals(mode)) {
        return;
      } else if ("overwrite".equals(mode)) {
        clientPool.run(
            client -> {
              client.dropDatabase(catalogName, dbName, false, true, false);
              return null;
            });
      }
    }

    // If no location is specified in properties, use root config
    Map<String, String> dbProperties =
        new HashMap<>(properties != null ? properties : new HashMap<>());
    if (!dbProperties.containsKey(Hive3NamespaceConfig.DATABASE_LOCATION_URI)) {
      String dbLocation = String.format("%s/%s", config.getRoot(), dbName);
      dbProperties.put(Hive3NamespaceConfig.DATABASE_LOCATION_URI, dbLocation);
    }

    Database database = new Database();
    database.setCatalogName(catalogName);
    database.setName(dbName);
    Hive3Util.setDatabaseProperties(database, () -> catalog.getLocationUri(), dbName, dbProperties);

    clientPool.run(
        client -> {
          client.createDatabase(database);
          return null;
        });
  }

  protected Optional<String> doDescribeTable(ObjectIdentifier id) {
    String catalog = id.levelAtListPos(0).toLowerCase();
    String db = id.levelAtListPos(1).toLowerCase();
    String table = id.levelAtListPos(2).toLowerCase();

    Optional<Table> hmsTable = Hive3Util.getTable(clientPool, catalog, db, table);
    if (!hmsTable.isPresent()) {
      return Optional.empty();
    }

    Hive3Util.validateLanceTable(hmsTable.get());
    return Optional.of(hmsTable.get().getSd().getLocation());
  }

  protected void doCreateTable(
      ObjectIdentifier id,
      Schema schema,
      String location,
      Map<String, String> properties,
      byte[] data) {
    String catalog = id.levelAtListPos(0).toLowerCase();
    String db = id.levelAtListPos(1).toLowerCase();
    String tableName = id.levelAtListPos(2).toLowerCase();

    try {
      Optional<Table> existing = Hive3Util.getTable(clientPool, catalog, db, tableName);
      if (existing.isPresent()) {
        throw new TableAlreadyExistsException(
            String.format("Table %s.%s.%s already exists", catalog, db, tableName));
      }

      Table table = new Table();
      table.setCatName(catalog);
      table.setDbName(db);
      table.setTableName(tableName);
      table.setTableType("EXTERNAL_TABLE");
      table.setPartitionKeys(Lists.newArrayList());

      StorageDescriptor sd = new StorageDescriptor();
      sd.setLocation(location);
      sd.setCols(Lists.newArrayList());
      sd.setInputFormat("com.lancedb.lance.mapred.LanceInputFormat");
      sd.setOutputFormat("com.lancedb.lance.mapred.LanceOutputFormat");
      sd.setSerdeInfo(new org.apache.hadoop.hive.metastore.api.SerDeInfo());
      table.setSd(sd);

      Map<String, String> params = Hive3Util.createLanceTableParams(properties);
      table.setParameters(params);

      clientPool.run(
          client -> {
            client.createTable(table);
            return null;
          });
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new InternalException("Fail to create table: " + e.getMessage());
    }

    if (data != null && data.length > 0) {
      WriteParams writeParams =
          new WriteParams.Builder()
              .withMode(WriteParams.WriteMode.CREATE)
              .withStorageOptions(config.getStorageOptions())
              .build();
      Dataset.create(allocator, location, schema, writeParams);
    }
  }

  protected List<String> doListTables(String catalog, String db) {
    try {
      // First validate that catalog and database exist
      Catalog catalogObj = Hive3Util.getCatalogOrNull(clientPool, catalog);
      if (catalogObj == null) {
        throw new NamespaceNotFoundException(String.format("Catalog %s doesn't exist", catalog));
      }

      Database database = Hive3Util.getDatabaseOrNull(clientPool, catalog, db);
      if (database == null) {
        throw new NamespaceNotFoundException(
            String.format("Database %s.%s doesn't exist", catalog, db));
      }

      List<String> allTables = clientPool.run(client -> client.getAllTables(catalog, db));
      List<String> lanceTables = Lists.newArrayList();

      for (String tableName : allTables) {
        try {
          Optional<Table> table = Hive3Util.getTable(clientPool, catalog, db, tableName);
          if (table.isPresent()) {
            Map<String, String> params = table.get().getParameters();
            if (params != null && "lance".equalsIgnoreCase(params.get("table_type"))) {
              lanceTables.add(tableName);
            }
          }
        } catch (Exception e) {
          // Skip tables that can't be accessed or validated
          LOG.warn("Failed to validate table {}.{}.{}: {}", catalog, db, tableName, e.getMessage());
        }
      }

      return lanceTables;
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new ServiceUnavailableException("Failed to list tables: " + errorMessage);
    }
  }

  protected String doDropTable(ObjectIdentifier id) {
    String catalog = id.levelAtListPos(0).toLowerCase();
    String db = id.levelAtListPos(1).toLowerCase();
    String tableName = id.levelAtListPos(2).toLowerCase();

    try {
      Optional<Table> hmsTable = Hive3Util.getTable(clientPool, catalog, db, tableName);
      if (!hmsTable.isPresent()) {
        throw new TableNotFoundException(
            String.format("Table %s.%s.%s does not exist", catalog, db, tableName));
      }

      Hive3Util.validateLanceTable(hmsTable.get());
      String location = hmsTable.get().getSd().getLocation();

      clientPool.run(
          client -> {
            client.dropTable(catalog, db, tableName, false, true);
            return null;
          });

      return location;
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new ServiceUnavailableException("Failed to drop table: " + errorMessage);
    }
  }

  protected Map<String, String> doDropNamespace(ObjectIdentifier id, String mode) {

    try {
      if (id.levels() == 1) {
        // Drop catalog
        return doDropCatalog(id.levelAtListPos(0).toLowerCase(), mode);
      } else {
        // Drop database
        return doDropDatabase(
            id.levelAtListPos(0).toLowerCase(), id.levelAtListPos(1).toLowerCase(), mode);
      }
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new ServiceUnavailableException("Failed to drop namespace: " + errorMessage);
    }
  }

  private Map<String, String> doDropCatalog(String catalog, String mode)
      throws TException, InterruptedException {
    Catalog catalogObj = Hive3Util.getCatalogOrNull(clientPool, catalog);
    if (catalogObj == null) {
      if ("skip".equals(mode)) {
        return new HashMap<>();
      } else {
        throw new NamespaceNotFoundException(String.format("Catalog %s doesn't exist", catalog));
      }
    }

    // Check for child databases (RESTRICT mode only)
    List<String> databases = clientPool.run(client -> client.getAllDatabases(catalog));
    if (!databases.isEmpty()) {
      throw new InvalidInputException(
          String.format(
              "Catalog %s is not empty. Contains %d databases: %s",
              catalog, databases.size(), databases));
    }

    // Collect catalog properties
    Map<String, String> properties = new HashMap<>();
    if (catalogObj.getDescription() != null) {
      properties.put("description", catalogObj.getDescription());
    }
    if (catalogObj.getLocationUri() != null) {
      properties.put("catalog.location.uri", catalogObj.getLocationUri());
    }

    // Drop the catalog
    clientPool.run(
        client -> {
          client.dropCatalog(catalog);
          return null;
        });

    LOG.info("Successfully dropped catalog: {}", catalog);
    return properties;
  }

  private Map<String, String> doDropDatabase(String catalog, String db, String mode)
      throws TException, InterruptedException {
    Database database = Hive3Util.getDatabaseOrNull(clientPool, catalog, db);
    if (database == null) {
      if ("skip".equals(mode)) {
        return new HashMap<>();
      } else {
        throw new NamespaceNotFoundException(
            String.format("Database %s.%s doesn't exist", catalog, db));
      }
    }

    // Check if database contains tables (RESTRICT mode only)
    List<String> tables = doListTables(catalog, db);
    if (!tables.isEmpty()) {
      throw new InvalidInputException(
          String.format(
              "Database %s.%s is not empty. Contains %d tables: %s",
              catalog, db, tables.size(), tables));
    }

    // Collect database properties
    Map<String, String> properties = new HashMap<>();
    if (database.getDescription() != null) {
      properties.put(Hive3NamespaceConfig.DATABASE_DESCRIPTION, database.getDescription());
    }
    if (database.getLocationUri() != null) {
      properties.put(Hive3NamespaceConfig.DATABASE_LOCATION_URI, database.getLocationUri());
    }
    if (database.getOwnerName() != null) {
      properties.put(Hive3NamespaceConfig.DATABASE_OWNER, database.getOwnerName());
    }
    if (database.getOwnerType() != null) {
      properties.put(Hive3NamespaceConfig.DATABASE_OWNER_TYPE, database.getOwnerType().name());
    }
    if (database.getParameters() != null) {
      properties.putAll(database.getParameters());
    }

    // Drop the database
    clientPool.run(
        client -> {
          client.dropDatabase(catalog, db, false, true, false);
          return null;
        });

    LOG.info("Successfully dropped database: {}.{}", catalog, db);
    return properties;
  }

  private String getDefaultTableLocation(String catalogName, String dbName, String tableName) {
    try {
      // Try to get the database location first
      Database db =
          Hive3Util.getDatabaseOrNull(clientPool, catalogName.toLowerCase(), dbName.toLowerCase());
      if (db != null && db.getLocationUri() != null && !db.getLocationUri().isEmpty()) {
        String dbLocation = db.getLocationUri();
        if (!dbLocation.endsWith("/")) {
          dbLocation += "/";
        }
        return dbLocation + tableName.toLowerCase() + ".lance";
      }
    } catch (Exception e) {
      // Fall back to using root config if database location fails
      LOG.warn(
          "Failed to get database location for {}.{}, using root config", catalogName, dbName, e);
    }

    // Use the configured root as fallback
    return String.format(
        "%s/%s/%s.lance", config.getRoot(), dbName.toLowerCase(), tableName.toLowerCase());
  }
}
