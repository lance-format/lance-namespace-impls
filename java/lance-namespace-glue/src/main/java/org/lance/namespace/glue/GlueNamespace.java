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
package org.lance.namespace.glue;

import org.lance.Dataset;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.NamespaceNotFoundException;
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
import org.lance.namespace.model.JsonArrowSchema;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.arrow.memory.BufferAllocator;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableVersionRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GlueNamespace implements LanceNamespace, Closeable {

  private static final String PARAM_LOCATION = "location";
  private static final String PARAM_DESCRIPTION = "description";
  public static final String TABLE_TYPE_PROP = "table_type";
  public static final String LANCE_TABLE_TYPE_VALUE = "lance";
  public static final String MANAGED_BY_PROP = "managed_by";
  public static final String STORAGE_VALUE = "storage";
  public static final String VERSION_PROP = "version";
  private static final int MAX_LISTING_SIZE = 100;

  private GlueNamespaceConfig config;
  private GlueClient glueClient;
  private BufferAllocator allocator;

  public GlueNamespace() {}

  @Override
  public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
    GlueNamespaceConfig glueProperties = new GlueNamespaceConfig(configProperties);
    GlueClient glueClient =
        GlueClient.builder().applyMutation(glueProperties::configureClientBuilder).build();
    initialize(glueProperties, glueClient, allocator);
  }

  @VisibleForTesting
  void initialize(
      GlueNamespaceConfig properties, GlueClient glueClient, BufferAllocator allocator) {
    this.config = properties;
    this.glueClient = glueClient;
    this.allocator = allocator;
  }

  @Override
  public String namespaceId() {
    String catalogId = config.catalogId() != null ? config.catalogId() : "default";
    String region =
        glueClient.serviceClientConfiguration().region() != null
            ? glueClient.serviceClientConfiguration().region().id()
            : "default";
    return String.format("GlueNamespace { catalogId: \"%s\", region: \"%s\" }", catalogId, region);
  }

  @Override
  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    validateParent(request.getId());

    GetDatabasesRequest.Builder listRequest =
        GetDatabasesRequest.builder().catalogId(config.catalogId());
    int pageSize = request.getLimit() != null ? request.getLimit() : Integer.MAX_VALUE;
    int remaining = pageSize;
    String glueNextToken = request.getPageToken();
    Set<String> databases = Sets.newHashSet();
    do {
      int fetchSize = Math.min(remaining, MAX_LISTING_SIZE);
      GetDatabasesResponse response =
          glueClient.getDatabases(
              listRequest.maxResults(fetchSize).nextToken(glueNextToken).build());
      response.databaseList().forEach(d -> databases.add(d.name()));
      glueNextToken = response.nextToken();
      remaining = pageSize - databases.size();
    } while (glueNextToken != null && remaining > 0);

    return new ListNamespacesResponse().namespaces(databases).pageToken(glueNextToken);
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
    String namespaceName = namespaceFromId(request.getId());

    Database database = getDatabase(namespaceName);
    Map<String, String> glueProperties = extractDatabaseProperties(database);
    return new DescribeNamespaceResponse().properties(glueProperties);
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    String namespaceName = namespaceFromId(request.getId());

    String mode = request.getMode() != null ? request.getMode().toLowerCase() : "create";
    Map<String, String> params =
        request.getProperties() != null ? request.getProperties() : ImmutableMap.of();
    boolean namespaceExists = databaseExists(namespaceName);

    if ("exist_ok".equals(mode) || "existok".equals(mode)) {
      if (namespaceExists) {
        return describeNamespaceAsCreateResponse(namespaceName);
      }
    } else if ("overwrite".equals(mode)) {
      if (namespaceExists) {
        deleteDatabase(namespaceName);
      }
    }

    try {
      glueClient.createDatabase(
          CreateDatabaseRequest.builder()
              .catalogId(config.catalogId())
              .databaseInput(buildDatabaseInput(namespaceName, params))
              .build());
      return new CreateNamespaceResponse().properties(params);
    } catch (AlreadyExistsException e) {
      if ("exist_ok".equals(mode) || "existok".equals(mode)) {
        return describeNamespaceAsCreateResponse(namespaceName);
      }
      throw GlueToLanceErrorConverter.conflict(e, "Namespace already exists: %s", namespaceName);
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to create namespace: %s", namespaceName);
    }
  }

  @Override
  public DropNamespaceResponse dropNamespace(DropNamespaceRequest request) {
    if ("Cascade".equalsIgnoreCase(request.getBehavior())) {
      throw new InvalidInputException("Cascade behavior is not supported for this implementation");
    }

    String namespaceName = namespaceFromId(request.getId());

    String mode = request.getMode() != null ? request.getMode().toLowerCase() : "fail";

    if (!databaseExists(namespaceName)) {
      if ("skip".equals(mode)) {
        return new DropNamespaceResponse();
      }
      throw new NamespaceNotFoundException(
          "Namespace not found: " + namespaceName, "NAMESPACE_NOT_FOUND", namespaceName);
    }

    ensureNamespaceEmpty(namespaceName);

    deleteDatabase(namespaceName);
    return new DropNamespaceResponse();
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    String namespaceName = namespaceFromId(request.getId());

    try {
      GetTablesRequest.Builder listRequest =
          GetTablesRequest.builder().catalogId(config.catalogId()).databaseName(namespaceName);

      int pageSize = request.getLimit() != null ? request.getLimit() : Integer.MAX_VALUE;
      int remaining = pageSize;
      String glueNextToken = request.getPageToken();
      Set<String> tables = Sets.newHashSet();

      do {
        int fetchSize = Math.min(remaining, MAX_LISTING_SIZE);
        GetTablesResponse response =
            glueClient.getTables(
                listRequest.maxResults(fetchSize).nextToken(glueNextToken).build());
        response.tableList().stream().filter(this::isLanceTable).forEach(t -> tables.add(t.name()));
        glueNextToken = response.nextToken();
        remaining = pageSize - tables.size();
      } while (glueNextToken != null && remaining > 0);
      return new ListTablesResponse().tables(tables).pageToken(glueNextToken);
    } catch (EntityNotFoundException e) {
      throw GlueToLanceErrorConverter.notFound(e, "Glue database not found: %s", namespaceName);
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to list tables in Glue database: %s", namespaceName);
    }
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    if (Boolean.TRUE.equals(request.getLoadDetailedMetadata())) {
      throw new InvalidInputException(
          "load_detailed_metadata=true is not supported for this implementation");
    }

    validateTableId(request.getId());
    String namespaceName = request.getId().get(0);
    String tableName = request.getId().get(1);

    Table table = getGlueTableAtVersion(namespaceName, tableName, request.getVersion());
    ensureLanceTable(table);

    DescribeTableResponse response = new DescribeTableResponse();
    if (table.storageDescriptor() != null && table.storageDescriptor().location() != null) {
      response.setLocation(table.storageDescriptor().location());
    }
    response.setStorageOptions(config.getStorageOptions());
    return response;
  }

  @Override
  public DeregisterTableResponse deregisterTable(DeregisterTableRequest request) {
    validateTableId(request.getId());
    String namespaceName = request.getId().get(0);
    String tableName = request.getId().get(1);

    try {
      Table table = getGlueTable(namespaceName, tableName);
      ensureLanceTable(table);
      deleteGlueTable(namespaceName, tableName, false);

      DeregisterTableResponse response = new DeregisterTableResponse();
      response.setId(request.getId());
      if (table.storageDescriptor() != null && table.storageDescriptor().location() != null) {
        response.setLocation(table.storageDescriptor().location());
      }
      if (table.parameters() != null && !table.parameters().isEmpty()) {
        response.setProperties(table.parameters());
      }
      return response;

    } catch (EntityNotFoundException e) {
      throw GlueToLanceErrorConverter.notFound(
          e, "Glue table not found: %s.%s", namespaceName, tableName);
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to deregister table: %s.%s", namespaceName, tableName);
    }
  }

  // Removed: createTable(CreateTableRequest, byte[]) - using default implementation from interface

  @Override
  public CreateEmptyTableResponse createEmptyTable(CreateEmptyTableRequest request) {
    validateTableId(request.getId());
    String namespaceName = request.getId().get(0);
    String tableName = request.getId().get(1);

    String location = request.getLocation();
    if (location == null || location.isEmpty()) {
      location = getDefaultTableLocation(namespaceName, tableName);
    }

    try {
      Map<String, String> params = Maps.newHashMap();
      params.put(TABLE_TYPE_PROP, LANCE_TABLE_TYPE_VALUE);
      params.put(MANAGED_BY_PROP, STORAGE_VALUE);

      TableInput tableInput =
          TableInput.builder()
              .name(tableName)
              .storageDescriptor(
                  StorageDescriptor.builder().location(location).parameters(params).build())
              .parameters(params)
              .build();

      glueClient.createTable(
          software.amazon.awssdk.services.glue.model.CreateTableRequest.builder()
              .catalogId(config.catalogId())
              .databaseName(namespaceName)
              .tableInput(tableInput)
              .build());

      CreateEmptyTableResponse response = new CreateEmptyTableResponse();
      response.setLocation(location);
      response.setStorageOptions(config.getStorageOptions());
      return response;
    } catch (AlreadyExistsException e) {
      throw GlueToLanceErrorConverter.tableConflict(
          e, "Table already exists: %s.%s", namespaceName, tableName);
    } catch (EntityNotFoundException e) {
      throw GlueToLanceErrorConverter.namespaceNotFound(
          e, "Namespace not found: %s", namespaceName);
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to create empty table: %s.%s", namespaceName, tableName);
    }
  }

  private void validateParent(List<String> id) {
    if (id != null && id.size() > 1) {
      String instance = String.join("/", id);
      throw new InvalidInputException(
          "Glue does not support nested namespaces. Found nested path: " + String.join("/", id),
          "BAD_REQUEST",
          instance);
    }
  }

  private String namespaceFromId(List<String> id) {
    if (id == null || id.isEmpty()) {
      throw new InvalidInputException("Namespace identifier cannot be null or empty");
    }

    validateParent(id);
    String namespace = id.get(0);
    if (namespace == null || namespace.isEmpty()) {
      throw new InvalidInputException("Namespace name cannot be empty");
    }
    return namespace;
  }

  private void validateTableId(List<String> id) {
    if (id == null || id.size() != 2) {
      throw new InvalidInputException(
          "Table identifier must contain exactly 2 elements, but got " + id,
          "BAD_REQUEST",
          id != null ? String.join("/", id) : "");
    }

    if (id.get(0) == null || id.get(0).isEmpty()) {
      throw new InvalidInputException("Namespace name cannot be empty");
    }
    if (id.get(1) == null || id.get(1).isEmpty()) {
      throw new InvalidInputException("Table name cannot be empty");
    }
  }

  private static Map<String, String> extractDatabaseProperties(Database database) {
    Map<String, String> glueProperties =
        database.parameters() != null ? Maps.newHashMap(database.parameters()) : Maps.newHashMap();
    if (database.locationUri() != null) {
      glueProperties.put(PARAM_LOCATION, database.locationUri());
    }
    if (database.description() != null) {
      glueProperties.put(PARAM_DESCRIPTION, database.description());
    }
    return glueProperties;
  }

  private boolean databaseExists(String namespaceName) {
    try {
      glueClient.getDatabase(
          GetDatabaseRequest.builder().catalogId(config.catalogId()).name(namespaceName).build());
      return true;
    } catch (EntityNotFoundException e) {
      return false;
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to get Glue database: %s", namespaceName);
    }
  }

  private Database getDatabase(String namespaceName) {
    try {
      return glueClient
          .getDatabase(
              GetDatabaseRequest.builder()
                  .catalogId(config.catalogId())
                  .name(namespaceName)
                  .build())
          .database();
    } catch (EntityNotFoundException e) {
      throw GlueToLanceErrorConverter.notFound(e, "Glue database not found: %s", namespaceName);
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to get Glue database: %s", namespaceName);
    }
  }

  private void deleteDatabase(String namespaceName) {
    try {
      glueClient.deleteDatabase(
          DeleteDatabaseRequest.builder()
              .catalogId(config.catalogId())
              .name(namespaceName)
              .build());
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to drop Glue namespace: %s", namespaceName);
    }
  }

  private DatabaseInput buildDatabaseInput(String namespaceName, Map<String, String> params) {
    DatabaseInput.Builder builder = DatabaseInput.builder().name(namespaceName);

    if (params.containsKey(PARAM_LOCATION)) {
      String location = params.get(PARAM_LOCATION);
      location =
          location != null && location.endsWith("/")
              ? location.substring(0, location.length() - 1)
              : location;
      builder.locationUri(location);
    }
    if (params.containsKey(PARAM_DESCRIPTION)) {
      builder.description(params.get(PARAM_DESCRIPTION));
    }

    Map<String, String> parameters = Maps.newHashMap(params);
    parameters.remove(PARAM_LOCATION);
    parameters.remove(PARAM_DESCRIPTION);
    if (!parameters.isEmpty()) {
      builder.parameters(parameters);
    }

    return builder.build();
  }

  private CreateNamespaceResponse describeNamespaceAsCreateResponse(String namespaceName) {
    Database existing = getDatabase(namespaceName);
    Map<String, String> properties = extractDatabaseProperties(existing);
    return new CreateNamespaceResponse().properties(properties);
  }

  private void deleteAllTables(String namespaceName) {
    try {
      String nextToken = null;
      do {
        GetTablesResponse tablesResponse =
            glueClient.getTables(
                GetTablesRequest.builder()
                    .catalogId(config.catalogId())
                    .databaseName(namespaceName)
                    .nextToken(nextToken)
                    .build());
        for (Table table : tablesResponse.tableList()) {
          if (table.storageDescriptor() != null && table.storageDescriptor().location() != null) {
            String tableLocation = table.storageDescriptor().location();
            tableLocation =
                tableLocation != null && tableLocation.endsWith("/")
                    ? tableLocation.substring(0, tableLocation.length() - 1)
                    : tableLocation;
            if (isLanceTable(table)) {
              try {
                Dataset.drop(tableLocation, config.getStorageOptions());
              } catch (Exception e) {
                throw new InternalException(
                    String.format("Failed to drop table: %s.%s", namespaceName, table.name()),
                    "TABLE_DROP_ERROR",
                    namespaceName + "." + table.name());
              }
            }
          }
          deleteGlueTable(namespaceName, table.name(), true);
        }
        nextToken = tablesResponse.nextToken();
      } while (nextToken != null);
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to delete tables in glue database: %s", namespaceName);
    }
  }

  private void ensureNamespaceEmpty(String namespaceName) {
    try {
      GetTablesResponse tablesResponse =
          glueClient.getTables(
              GetTablesRequest.builder()
                  .catalogId(config.catalogId())
                  .databaseName(namespaceName)
                  .build());
      if (!tablesResponse.tableList().isEmpty()) {
        throw new InvalidInputException(
            "Namespace not empty: " + namespaceName, "BAD_REQUEST", namespaceName);
      }
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to ensure Glue database is empty: %s", namespaceName);
    }
  }

  private boolean isLanceTable(Table table) {
    if (table == null || table.parameters() == null) {
      return false;
    }
    return LANCE_TABLE_TYPE_VALUE.equalsIgnoreCase(table.parameters().get(TABLE_TYPE_PROP));
  }

  private void ensureLanceTable(Table table) {
    if (!isLanceTable(table)) {
      throw new TableNotFoundException(
          String.format("Table not found: %s.%s", table.databaseName(), table.name()),
          "NOT_LANCE_TABLE",
          table.databaseName() + "." + table.name());
    }
  }

  private String getDefaultTableLocation(String namespaceName, String tableName) {
    Database db = getDatabase(namespaceName);
    String dbUri = db.locationUri();

    if (dbUri == null || dbUri.isEmpty()) {
      String rootConfig = config.getRoot();
      if (rootConfig == null) {
        throw new IllegalStateException(
            "Root configuration is null - cannot derive table location");
      }
      return String.format("%s/%s/%s.lance", rootConfig, namespaceName, tableName);
    }
    String base = dbUri;
    base = base != null && base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
    return String.format("%s/%s.lance", base, tableName);
  }

  private Table getGlueTable(String namespace, String tableName) {
    try {
      return glueClient
          .getTable(
              GetTableRequest.builder()
                  .catalogId(config.catalogId())
                  .databaseName(namespace)
                  .name(tableName)
                  .build())
          .table();
    } catch (EntityNotFoundException e) {
      throw GlueToLanceErrorConverter.notFound(
          e, "Glue table not found: %s.%s", namespace, tableName);
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to get Glue table: %s.%s", namespace, tableName);
    }
  }

  private void deleteGlueTable(String namespaceName, String tableName, boolean failIfNotFound) {
    try {
      glueClient.deleteTable(
          DeleteTableRequest.builder()
              .catalogId(config.catalogId())
              .databaseName(namespaceName)
              .name(tableName)
              .build());
    } catch (EntityNotFoundException e) {
      if (failIfNotFound) {
        throw GlueToLanceErrorConverter.notFound(
            e, "Glue table not found: %s.%s", namespaceName, tableName);
      }
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to delete Glue table: %s.%s", namespaceName, tableName);
    }
  }

  private Table getGlueTableAtVersion(String namespaceName, String tableName, Long version) {
    try {
      Table table;
      if (version != null) {
        // Get specific table version
        String tableVersion = String.valueOf(version);
        table =
            glueClient
                .getTableVersion(
                    GetTableVersionRequest.builder()
                        .catalogId(config.catalogId())
                        .databaseName(namespaceName)
                        .tableName(tableName)
                        .versionId(tableVersion)
                        .build())
                .tableVersion()
                .table();
      } else {
        // Get current table version
        table = getGlueTable(namespaceName, tableName);
      }
      return table;
    } catch (EntityNotFoundException e) {
      throw GlueToLanceErrorConverter.notFound(
          e, "Glue table not found: %s.%s", namespaceName, tableName);
    } catch (GlueException e) {
      throw GlueToLanceErrorConverter.serverError(
          e, "Failed to get Glue table: %s.%s", namespaceName, tableName);
    }
  }

  private void validateSchemaNotNull(
      JsonArrowSchema schema, String namespaceName, String tableName) {
    if (schema == null) {
      throw new InvalidInputException(
          String.format(
              "Schema is required in CreateTableRequest: %s.%s", namespaceName, tableName),
          "BAD_REQUEST",
          namespaceName + "." + tableName);
    }
  }

  private void safeDropDataset(String location) {
    try {
      Dataset.drop(location, config.getStorageOptions());
    } catch (Exception cleanupErr) {
      // failed to clean up dataset
    }
  }

  @Override
  public void close() {
    if (glueClient != null) {
      glueClient.close();
    }
  }
}
