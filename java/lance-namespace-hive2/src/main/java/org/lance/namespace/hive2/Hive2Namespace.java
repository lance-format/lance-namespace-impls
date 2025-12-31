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
package org.lance.namespace.hive2;

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
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
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
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
import java.util.function.Supplier;

import static org.lance.namespace.hive2.Hive2ErrorType.DatabaseAlreadyExist;
import static org.lance.namespace.hive2.Hive2ErrorType.HiveMetaStoreError;
import static org.lance.namespace.hive2.Hive2ErrorType.TableAlreadyExists;
import static org.lance.namespace.hive2.Hive2ErrorType.TableNotFound;

public class Hive2Namespace implements LanceNamespace {
  private static final Logger LOG = LoggerFactory.getLogger(Hive2Namespace.class);

  private Hive2ClientPool clientPool;
  private Configuration hadoopConf;
  private BufferAllocator allocator;
  private Hive2NamespaceConfig config;

  public Hive2Namespace() {}

  /**
   * Set the Hadoop configuration. Must be called before initialize() if a custom configuration is
   * needed (e.g., for connecting to a specific Hive Metastore).
   *
   * @param conf The Hadoop configuration to use
   */
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
    this.config = new Hive2NamespaceConfig(configProperties);
    this.clientPool = new Hive2ClientPool(config.getClientPoolSize(), hadoopConf);
  }

  @Override
  public String namespaceId() {
    String metastoreUri =
        hadoopConf != null
            ? hadoopConf.get(HiveConf.ConfVars.METASTOREURIS.varname, "default")
            : "default";
    return String.format("Hive2Namespace { metastoreUri: \"%s\" }", metastoreUri);
  }

  @Override
  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(nsId.levels() <= 1, "Expect a 1-level namespace but get %s", nsId);

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
        !id.isRoot() && id.levels() <= 1, "Expect a 1-level namespace but get %s", id);

    doCreateNamespace(id, mode, properties);

    CreateNamespaceResponse response = new CreateNamespaceResponse();
    response.setProperties(properties);
    return response;
  }

  @Override
  public void namespaceExists(NamespaceExistsRequest request) {
    ObjectIdentifier id = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        !id.isRoot() && id.levels() <= 1, "Expect a 1-level namespace but get %s", id);

    String db = id.levelAtListPos(0).toLowerCase();
    Database database = Hive2Util.getDatabaseOrNull(clientPool, db);

    if (database == null) {
      throw new NamespaceNotFoundException(
          String.format("Namespace does not exist: %s", id.stringStyleId()),
          HiveMetaStoreError.getType(),
          id.stringStyleId());
    }
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
    ObjectIdentifier id = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        !id.isRoot() && id.levels() <= 1, "Expect a 1-level namespace but get %s", id);

    String db = id.levelAtListPos(0).toLowerCase();
    Database database = Hive2Util.getDatabaseOrNull(clientPool, db);

    if (database == null) {
      throw new NamespaceNotFoundException(
          String.format("Namespace does not exist: %s", id.stringStyleId()),
          HiveMetaStoreError.getType(),
          id.stringStyleId());
    }

    DescribeNamespaceResponse response = new DescribeNamespaceResponse();
    Map<String, String> properties = new HashMap<>();

    if (database.getDescription() != null) {
      properties.put(Hive2NamespaceConfig.DATABASE_DESCRIPTION, database.getDescription());
    }
    if (database.getLocationUri() != null) {
      properties.put(Hive2NamespaceConfig.DATABASE_LOCATION_URI, database.getLocationUri());
    }
    if (database.getOwnerName() != null) {
      properties.put("owner", database.getOwnerName());
    }
    if (database.getOwnerType() != null) {
      properties.put("owner_type", database.getOwnerType().name());
    }

    if (database.getParameters() != null) {
      properties.putAll(database.getParameters());
    }

    response.setProperties(properties);
    return response;
  }

  @Override
  public DropNamespaceResponse dropNamespace(DropNamespaceRequest request) {
    ObjectIdentifier id = ObjectIdentifier.of(request.getId());
    String mode = request.getMode() != null ? request.getMode().toLowerCase() : "fail";
    String behavior = request.getBehavior() != null ? request.getBehavior() : "Restrict";

    ValidationUtil.checkArgument(
        !id.isRoot() && id.levels() <= 1, "Expect a 1-level namespace but get %s", id);

    Map<String, String> properties = doDropNamespace(id, mode, behavior);

    DropNamespaceResponse response = new DropNamespaceResponse();
    response.setProperties(properties);
    return response;
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        !nsId.isRoot() && nsId.levels() == 1, "Expect a 1-level namespace but get %s", nsId);

    String db = nsId.levelAtListPos(0).toLowerCase();
    List<String> tables = doListTables(db);

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
  public void tableExists(TableExistsRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        tableId.levels() == 2, "Expect 2-level table identifier but get %s", tableId);

    String db = tableId.levelAtListPos(0).toLowerCase();
    String table = tableId.levelAtListPos(1).toLowerCase();

    Optional<Table> hmsTable = Hive2Util.getTable(clientPool, db, table);

    if (!hmsTable.isPresent()) {
      throw new TableNotFoundException(
          String.format("Table does not exist: %s", tableId.stringStyleId()),
          TableNotFound.getType(),
          tableId.stringStyleId());
    }

    Hive2Util.validateLanceTable(hmsTable.get());
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        tableId.levels() == 2, "Expect 2-level table identifier but get %s", tableId);

    Optional<String> location = doDescribeTable(tableId);

    if (!location.isPresent()) {
      throw new TableNotFoundException(
          String.format("Table does not exist: %s", tableId.stringStyleId()),
          TableNotFound.getType(),
          tableId.stringStyleId());
    }

    DescribeTableResponse response = new DescribeTableResponse();
    response.setLocation(location.get());
    response.setStorageOptions(config.getStorageOptions());
    return response;
  }

  // Removed: createTable(CreateTableRequest, byte[]) - using default implementation from interface

  @Override
  public CreateEmptyTableResponse createEmptyTable(CreateEmptyTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        tableId.levels() == 2, "Expect 2-level table identifier but get %s", tableId);

    String location = request.getLocation();
    if (location == null || location.isEmpty()) {
      location = getDefaultTableLocation(tableId.levelAtListPos(0), tableId.levelAtListPos(1));
    }

    // Create table in metastore without data (pass null for requestData)
    doCreateTable(tableId, null, location, null, null);

    CreateEmptyTableResponse response = new CreateEmptyTableResponse();
    response.setLocation(location);
    response.setStorageOptions(config.getStorageOptions());
    return response;
  }

  @Override
  public DeregisterTableResponse deregisterTable(DeregisterTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        tableId.levels() == 2, "Expect 2-level table identifier but get %s", tableId);

    String location = doDropTable(tableId);

    DeregisterTableResponse response = new DeregisterTableResponse();
    response.setId(request.getId());
    response.setLocation(location);
    return response;
  }

  public void setConf(Configuration conf) {
    this.hadoopConf = conf;
  }

  protected List<String> doListNamespaces(ObjectIdentifier parent) {
    try {
      if (parent.isRoot()) {
        return clientPool.run(IMetaStoreClient::getAllDatabases);
      } else {
        return Lists.newArrayList();
      }
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new ServiceUnavailableException(
          "Failed to list namespaces: " + errorMessage, HiveMetaStoreError.getType(), "");
    }
  }

  protected void doCreateNamespace(
      ObjectIdentifier id, String mode, Map<String, String> properties) {

    try {
      String db = id.levelAtListPos(0).toLowerCase();
      createDatabase(db, mode, properties);
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new ServiceUnavailableException(
          "Failed to create namespace: " + errorMessage, HiveMetaStoreError.getType(), "");
    }
  }

  private void createDatabase(String dbName, String mode, Map<String, String> properties)
      throws TException, InterruptedException {
    Database oldDb = Hive2Util.getDatabaseOrNull(clientPool, dbName);
    if (oldDb != null) {
      if ("create".equals(mode)) {
        throw new NamespaceAlreadyExistsException(
            String.format("Database %s already exist", dbName), DatabaseAlreadyExist.getType(), "");
      } else if ("exist_ok".equals(mode) || "existok".equals(mode)) {
        return;
      } else if ("overwrite".equals(mode)) {
        clientPool.run(
            client -> {
              client.dropDatabase(dbName, false, true, false);
              return null;
            });
      }
    }

    // Create database
    Supplier<String> warehouseLocation =
        () ->
            ValidationUtil.checkNotNullOrEmptyString(
                hadoopConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
                String.format(
                    "Warehouse location is not set: %s=null",
                    HiveConf.ConfVars.METASTOREWAREHOUSE.varname));

    // If no location is specified in properties, use root config
    Map<String, String> dbProperties =
        new HashMap<>(properties != null ? properties : new HashMap<>());
    if (!dbProperties.containsKey(Hive2NamespaceConfig.DATABASE_LOCATION_URI)) {
      String dbLocation = String.format("%s/%s", config.getRoot(), dbName);
      dbProperties.put(Hive2NamespaceConfig.DATABASE_LOCATION_URI, dbLocation);
    }

    Database database = new Database();
    database.setName(dbName);
    Hive2Util.setDatabaseProperties(database, warehouseLocation, dbName, dbProperties);

    clientPool.run(
        client -> {
          client.createDatabase(database);
          return null;
        });
  }

  protected Optional<String> doDescribeTable(ObjectIdentifier id) {
    String db = id.levelAtListPos(0).toLowerCase();
    String table = id.levelAtListPos(1).toLowerCase();

    Optional<Table> hmsTable = Hive2Util.getTable(clientPool, db, table);
    if (!hmsTable.isPresent()) {
      return Optional.empty();
    }

    Hive2Util.validateLanceTable(hmsTable.get());
    return Optional.of(hmsTable.get().getSd().getLocation());
  }

  protected void doCreateTable(
      ObjectIdentifier id,
      Schema schema,
      String location,
      Map<String, String> properties,
      byte[] data) {

    if (properties != null && "impl".equalsIgnoreCase(properties.get("managed_by"))) {
      throw new UnsupportedOperationException("managed_by=impl is not supported yet");
    }

    String db = id.levelAtListPos(0).toLowerCase();
    String tableName = id.levelAtListPos(1).toLowerCase();

    try {
      Optional<Table> existing = Hive2Util.getTable(clientPool, db, tableName);
      if (existing.isPresent()) {
        throw new TableAlreadyExistsException(
            String.format("Table %s.%s already exists", db, tableName),
            TableAlreadyExists.getType(),
            String.format("%s.%s", db, tableName));
      }

      Table table = new Table();
      table.setDbName(db);
      table.setTableName(tableName);
      table.setTableType("EXTERNAL_TABLE");
      table.setPartitionKeys(Lists.newArrayList());

      StorageDescriptor sd = new StorageDescriptor();
      sd.setLocation(location);
      sd.setCols(Lists.newArrayList());
      sd.setInputFormat("com.lancedb.lance.mapred.LanceInputFormat");
      sd.setOutputFormat("com.lancedb.lance.mapred.LanceOutputFormat");
      org.apache.hadoop.hive.metastore.api.SerDeInfo serdeInfo =
          new org.apache.hadoop.hive.metastore.api.SerDeInfo();
      serdeInfo.setSerializationLib("com.lancedb.lance.mapred.LanceSerDe");
      sd.setSerdeInfo(serdeInfo);
      table.setSd(sd);

      Map<String, String> params = Hive2Util.createLanceTableParams(properties);
      table.setParameters(params);

      clientPool.run(
          client -> {
            client.createTable(table);
            return null;
          });

      if (data != null && data.length > 0) {
        WriteParams writeParams =
            new WriteParams.Builder()
                .withMode(WriteParams.WriteMode.CREATE)
                .withStorageOptions(config.getStorageOptions())
                .build();
        Dataset.create(allocator, location, schema, writeParams);
      }
    } catch (TException | InterruptedException | RuntimeException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new ServiceUnavailableException(
          "Failed to create table: " + errorMessage, HiveMetaStoreError.getType(), "");
    }
  }

  protected List<String> doListTables(String db) {
    try {
      // First validate that database exists
      Database database = Hive2Util.getDatabaseOrNull(clientPool, db);
      if (database == null) {
        throw new NamespaceNotFoundException(
            String.format("Database %s doesn't exist", db), HiveMetaStoreError.getType(), db);
      }

      List<String> allTables = clientPool.run(client -> client.getAllTables(db));
      List<String> lanceTables = Lists.newArrayList();

      for (String tableName : allTables) {
        try {
          Optional<Table> table = Hive2Util.getTable(clientPool, db, tableName);
          if (table.isPresent()) {
            Map<String, String> params = table.get().getParameters();
            if (params != null && "lance".equalsIgnoreCase(params.get("table_type"))) {
              lanceTables.add(tableName);
            }
          }
        } catch (Exception e) {
          // Skip tables that can't be accessed or validated
          LOG.warn("Failed to validate table {}.{}: {}", db, tableName, e.getMessage());
        }
      }

      return lanceTables;
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new ServiceUnavailableException(
          "Failed to list tables: " + errorMessage, HiveMetaStoreError.getType(), "");
    }
  }

  protected String doDropTable(ObjectIdentifier id) {
    String db = id.levelAtListPos(0).toLowerCase();
    String tableName = id.levelAtListPos(1).toLowerCase();

    try {
      Optional<Table> hmsTable = Hive2Util.getTable(clientPool, db, tableName);
      if (!hmsTable.isPresent()) {
        throw new TableNotFoundException(
            String.format("Table %s.%s does not exist", db, tableName),
            TableNotFound.getType(),
            String.format("%s.%s", db, tableName));
      }

      Hive2Util.validateLanceTable(hmsTable.get());
      String location = hmsTable.get().getSd().getLocation();

      clientPool.run(
          client -> {
            client.dropTable(db, tableName, false, true);
            return null;
          });

      return location;
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new ServiceUnavailableException(
          "Failed to drop table: " + errorMessage, HiveMetaStoreError.getType(), "");
    }
  }

  protected Map<String, String> doDropNamespace(ObjectIdentifier id, String mode, String behavior) {
    String db = id.levelAtListPos(0).toLowerCase();

    try {
      Database database = Hive2Util.getDatabaseOrNull(clientPool, db);
      if (database == null) {
        if ("skip".equals(mode)) {
          return new HashMap<>();
        } else {
          throw new NamespaceNotFoundException(
              String.format("Database %s doesn't exist", db), HiveMetaStoreError.getType(), db);
        }
      }

      // Check if database contains tables (RESTRICT behavior only, not for Cascade)
      boolean cascade = "Cascade".equalsIgnoreCase(behavior);
      if (!cascade) {
        List<String> tables = doListTables(db);
        if (!tables.isEmpty()) {
          throw new InvalidInputException(
              String.format(
                  "Database %s is not empty. Contains %d tables: %s", db, tables.size(), tables),
              HiveMetaStoreError.getType(),
              db);
        }
      }

      // Collect database properties before dropping
      Map<String, String> properties = new HashMap<>();
      if (database.getDescription() != null) {
        properties.put(Hive2NamespaceConfig.DATABASE_DESCRIPTION, database.getDescription());
      }
      if (database.getLocationUri() != null) {
        properties.put(Hive2NamespaceConfig.DATABASE_LOCATION_URI, database.getLocationUri());
      }
      if (database.getOwnerName() != null) {
        properties.put("owner", database.getOwnerName());
      }
      if (database.getOwnerType() != null) {
        properties.put("owner_type", database.getOwnerType().name());
      }
      if (database.getParameters() != null) {
        properties.putAll(database.getParameters());
      }

      // Drop the database
      final boolean cascadeDrop = cascade;
      clientPool.run(
          client -> {
            client.dropDatabase(db, false, true, cascadeDrop);
            return null;
          });

      LOG.info("Successfully dropped database: {}", db);
      return properties;
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new ServiceUnavailableException(
          "Failed to drop namespace: " + errorMessage, HiveMetaStoreError.getType(), db);
    }
  }

  private String getDefaultTableLocation(String namespaceName, String tableName) {
    try {
      // Try to get the database location first
      Database db = Hive2Util.getDatabaseOrNull(clientPool, namespaceName.toLowerCase());
      if (db != null && db.getLocationUri() != null && !db.getLocationUri().isEmpty()) {
        String dbLocation = db.getLocationUri();
        if (!dbLocation.endsWith("/")) {
          dbLocation += "/";
        }
        return dbLocation + tableName.toLowerCase() + ".lance";
      }
    } catch (Exception e) {
      // Fall back to using root config if database location fails
      LOG.warn("Failed to get database location for {}, using root config", namespaceName, e);
    }

    // Use the configured root as fallback
    return String.format(
        "%s/%s/%s.lance", config.getRoot(), namespaceName.toLowerCase(), tableName.toLowerCase());
  }
}
