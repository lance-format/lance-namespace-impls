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
package org.lance.namespace.unity;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
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
import org.lance.namespace.rest.RestClient;
import org.lance.namespace.rest.RestClientException;
import org.lance.namespace.util.ObjectIdentifier;
import org.lance.namespace.util.ValidationUtil;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Unity Catalog namespace implementation for Lance. */
public class UnityNamespace implements LanceNamespace, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(UnityNamespace.class);
  private static final String TABLE_TYPE_LANCE = "lance";
  private static final String TABLE_TYPE_EXTERNAL = "EXTERNAL";
  private static final String TABLE_TYPE_KEY = "table_type";

  private UnityNamespaceConfig config;
  private RestClient restClient;
  private BufferAllocator allocator;

  public UnityNamespace() {}

  @Override
  public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
    this.allocator = allocator;
    this.config = new UnityNamespaceConfig(configProperties);

    RestClient.Builder clientBuilder =
        RestClient.builder()
            .baseUrl(config.getFullApiUrl())
            .connectTimeout(config.getConnectTimeout(), TimeUnit.MILLISECONDS)
            .readTimeout(config.getReadTimeout(), TimeUnit.MILLISECONDS)
            .maxRetries(config.getMaxRetries());

    if (config.getAuthToken() != null) {
      clientBuilder.authToken(config.getAuthToken());
    }

    this.restClient = clientBuilder.build();
    LOG.info("Initialized Unity namespace with endpoint: {}", config.getEndpoint());
  }

  @Override
  public String namespaceId() {
    String endpoint = this.config.getEndpoint();
    String catalog = this.config.getCatalog();
    return String.format("UnityNamespace { endpoint: \"%s\", catalog: \"%s\" }", endpoint, catalog);
  }

  @Override
  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        nsId.levels() <= 2, "Expect at most 2-level namespace but get %s", nsId);

    try {
      List<String> namespaces;

      if (nsId.levels() == 0) {
        namespaces = Collections.singletonList(config.getCatalog());
      } else if (nsId.levels() == 1) {
        String catalog = nsId.levelAtListPos(0);
        if (!catalog.equals(config.getCatalog())) {
          throw new NamespaceNotFoundException(
              "Catalog not found. Expected: " + config.getCatalog());
        }

        String path = "/schemas?catalog_name=" + catalog;
        if (request.getLimit() != null) {
          path += "&max_results=" + request.getLimit();
        }
        if (request.getPageToken() != null) {
          path += "&page_token=" + request.getPageToken();
        }

        UnityModels.ListSchemasResponse response =
            restClient.get(path, UnityModels.ListSchemasResponse.class);

        if (response != null && response.getSchemas() != null) {
          namespaces =
              response.getSchemas().stream()
                  .map(UnityModels.SchemaInfo::getName)
                  .collect(Collectors.toList());
        } else {
          namespaces = Collections.emptyList();
        }
      } else {
        namespaces = Collections.emptyList();
      }

      Collections.sort(namespaces);
      Set<String> resultNamespaces = new LinkedHashSet<>(namespaces);

      ListNamespacesResponse response = new ListNamespacesResponse();
      response.setNamespaces(resultNamespaces);
      return response;

    } catch (RestClientException e) {
      throw new InternalException("Failed to list namespaces: " + e.getMessage());
    }
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(nsId.levels() == 2, "Expect a 2-level namespace but get %s", nsId);

    String catalog = nsId.levelAtListPos(0);
    String schema = nsId.levelAtListPos(1);

    if (!catalog.equals(config.getCatalog())) {
      throw new InvalidInputException(
          "Cannot create namespace in catalog. Expected: " + config.getCatalog());
    }

    try {
      UnityModels.CreateSchema createSchema = new UnityModels.CreateSchema();
      createSchema.setName(schema);
      createSchema.setCatalogName(catalog);
      createSchema.setProperties(request.getProperties());

      UnityModels.SchemaInfo schemaInfo =
          restClient.post("/schemas", createSchema, UnityModels.SchemaInfo.class);

      CreateNamespaceResponse response = new CreateNamespaceResponse();
      response.setProperties(schemaInfo.getProperties());
      return response;

    } catch (RestClientException e) {
      if (e.isConflict()) {
        throw new NamespaceAlreadyExistsException(
            "Namespace already exists: " + request.getId().toString());
      }
      throw new InternalException("Failed to create namespace: " + e.getMessage());
    }
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(nsId.levels() == 2, "Expect a 2-level namespace but get %s", nsId);

    String catalog = nsId.levelAtListPos(0);
    String schema = nsId.levelAtListPos(1);

    if (!catalog.equals(config.getCatalog())) {
      throw new NamespaceNotFoundException(
          "Catalog not found: " + catalog + ". Expected: " + config.getCatalog());
    }

    try {
      String fullName = catalog + "." + schema;
      UnityModels.SchemaInfo schemaInfo =
          restClient.get("/schemas/" + fullName, UnityModels.SchemaInfo.class);

      DescribeNamespaceResponse response = new DescribeNamespaceResponse();
      response.setProperties(schemaInfo.getProperties());
      return response;

    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new NamespaceNotFoundException("Namespace not found: " + request.getId().toString());
      }
      throw new InternalException("Failed to describe namespace: " + e.getMessage());
    }
  }

  @Override
  public void namespaceExists(NamespaceExistsRequest request) {
    describeNamespace(new DescribeNamespaceRequest().id(request.getId()));
  }

  @Override
  public DropNamespaceResponse dropNamespace(DropNamespaceRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(nsId.levels() == 2, "Expect a 2-level namespace but get %s", nsId);

    String catalog = nsId.levelAtListPos(0);
    String schema = nsId.levelAtListPos(1);

    if (!catalog.equals(config.getCatalog())) {
      throw new InvalidInputException(
          "Cannot drop namespace in catalog. Expected: " + config.getCatalog());
    }

    try {
      String fullName = catalog + "." + schema;
      String path = "/schemas/" + fullName;

      restClient.delete(path);
      return new DropNamespaceResponse();

    } catch (RestClientException e) {
      if (e.isNotFound()) {
        return new DropNamespaceResponse();
      }
      throw new InternalException("Failed to drop namespace: " + e.getMessage());
    }
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(nsId.levels() == 2, "Expect a 2-level namespace but get %s", nsId);

    String catalog = nsId.levelAtListPos(0);
    String schema = nsId.levelAtListPos(1);

    if (!catalog.equals(config.getCatalog())) {
      throw new NamespaceNotFoundException(
          "Catalog not found: " + catalog + ". Expected: " + config.getCatalog());
    }

    try {
      String path = "/tables?catalog_name=" + catalog + "&schema_name=" + schema;
      if (request.getLimit() != null) {
        path += "&max_results=" + request.getLimit();
      }
      if (request.getPageToken() != null) {
        path += "&page_token=" + request.getPageToken();
      }

      UnityModels.ListTablesResponse unityResponse =
          restClient.get(path, UnityModels.ListTablesResponse.class);

      List<String> tables = Collections.emptyList();
      if (unityResponse != null && unityResponse.getTables() != null) {
        tables =
            unityResponse.getTables().stream()
                .filter(this::isLanceTable)
                .map(UnityModels.TableInfo::getName)
                .collect(Collectors.toList());
      }

      Collections.sort(tables);
      Set<String> resultTables = new LinkedHashSet<>(tables);

      ListTablesResponse response = new ListTablesResponse();
      response.setTables(resultTables);
      return response;

    } catch (RestClientException e) {
      throw new InternalException("Failed to list tables: " + e.getMessage());
    }
  }

  @Override
  public CreateEmptyTableResponse createEmptyTable(CreateEmptyTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() == 3, "Expect a 3-level table identifier but get %s", tableId);

    String catalog = tableId.levelAtListPos(0);
    String schema = tableId.levelAtListPos(1);
    String table = tableId.levelAtListPos(2);

    if (!catalog.equals(config.getCatalog())) {
      throw new InvalidInputException(
          "Cannot create empty table in catalog. Expected: " + config.getCatalog());
    }

    try {
      String tablePath = request.getLocation();
      if (tablePath == null || tablePath.isEmpty()) {
        tablePath = config.getRoot() + "/" + catalog + "/" + schema + "/" + table;
      }

      UnityModels.CreateTable createTable = new UnityModels.CreateTable();
      createTable.setName(table);
      createTable.setCatalogName(catalog);
      createTable.setSchemaName(schema);
      createTable.setTableType(TABLE_TYPE_EXTERNAL);
      createTable.setDataSourceFormat("TEXT");

      List<UnityModels.ColumnInfo> columns = new ArrayList<>();
      UnityModels.ColumnInfo idColumn = new UnityModels.ColumnInfo();
      idColumn.setName("__placeholder_id");
      idColumn.setTypeText("BIGINT");
      idColumn.setTypeName("BIGINT");
      idColumn.setTypeJson("{\"type\":\"long\"}");
      idColumn.setPosition(0);
      idColumn.setNullable(true);
      columns.add(idColumn);
      createTable.setColumns(columns);
      createTable.setStorageLocation(tablePath);

      Map<String, String> properties = new HashMap<>();
      properties.put(TABLE_TYPE_KEY, TABLE_TYPE_LANCE);
      createTable.setProperties(properties);

      UnityModels.TableInfo tableInfo =
          restClient.post("/tables", createTable, UnityModels.TableInfo.class);

      CreateEmptyTableResponse response = new CreateEmptyTableResponse();
      response.setLocation(tablePath);
      return response;

    } catch (RestClientException e) {
      if (e.isConflict()) {
        throw new TableAlreadyExistsException(
            "Table already exists: " + request.getId().toString());
      }
      throw new InternalException("Failed to create empty table: " + e.getMessage());
    }
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() == 3, "Expect a 3-level table identifier but get %s", tableId);

    String catalog = tableId.levelAtListPos(0);
    String schema = tableId.levelAtListPos(1);
    String table = tableId.levelAtListPos(2);

    if (!catalog.equals(config.getCatalog())) {
      throw new NamespaceNotFoundException(
          "Catalog not found: " + catalog + ". Expected: " + config.getCatalog());
    }

    try {
      String fullName = catalog + "." + schema + "." + table;
      UnityModels.TableInfo tableInfo =
          restClient.get("/tables/" + fullName, UnityModels.TableInfo.class);

      if (!isLanceTable(tableInfo)) {
        throw new InvalidInputException("Not a Lance table: " + request.getId().toString());
      }

      DescribeTableResponse response = new DescribeTableResponse();
      response.setLocation(tableInfo.getStorageLocation());
      response.setStorageOptions(tableInfo.getProperties());
      return response;

    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new TableNotFoundException("Table not found: " + request.getId().toString());
      }
      throw new InternalException("Failed to describe table: " + e.getMessage());
    }
  }

  @Override
  public void tableExists(TableExistsRequest request) {
    describeTable(new DescribeTableRequest().id(request.getId()));
  }

  @Override
  public DeregisterTableResponse deregisterTable(DeregisterTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() == 3, "Expect a 3-level table identifier but get %s", tableId);

    String catalog = tableId.levelAtListPos(0);
    String schema = tableId.levelAtListPos(1);
    String table = tableId.levelAtListPos(2);

    if (!catalog.equals(config.getCatalog())) {
      throw new NamespaceNotFoundException(
          "Catalog not found: " + catalog + ". Expected: " + config.getCatalog());
    }

    try {
      String fullName = catalog + "." + schema + "." + table;
      UnityModels.TableInfo tableInfo =
          restClient.get("/tables/" + fullName, UnityModels.TableInfo.class);

      if (!isLanceTable(tableInfo)) {
        throw new InvalidInputException("Not a Lance table: " + request.getId().toString());
      }

      String location = tableInfo.getStorageLocation();
      restClient.delete("/tables/" + fullName);

      DeregisterTableResponse response = new DeregisterTableResponse();
      response.setLocation(location);
      return response;

    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new TableNotFoundException("Table not found: " + request.getId().toString());
      }
      throw new InternalException("Failed to deregister table: " + e.getMessage());
    }
  }

  @Override
  public void close() throws IOException {
    if (restClient != null) {
      restClient.close();
    }
  }

  private boolean isLanceTable(UnityModels.TableInfo tableInfo) {
    if (tableInfo == null || tableInfo.getProperties() == null) {
      return false;
    }
    String tableType = tableInfo.getProperties().get(TABLE_TYPE_KEY);
    return TABLE_TYPE_LANCE.equalsIgnoreCase(tableType);
  }

  private List<UnityModels.ColumnInfo> convertArrowSchemaToUnityColumns(Schema arrowSchema) {
    List<UnityModels.ColumnInfo> columns = new ArrayList<>();
    for (Field field : arrowSchema.getFields()) {
      UnityModels.ColumnInfo columnInfo = new UnityModels.ColumnInfo();
      columnInfo.setName(field.getName());
      String unityType = convertArrowTypeToUnityType(field.getType());
      columnInfo.setTypeText(unityType);
      columnInfo.setTypeJson(convertArrowTypeToUnityTypeJson(field.getType()));
      columnInfo.setTypeName(unityType);
      columnInfo.setPosition(columns.size());
      columnInfo.setNullable(field.isNullable());
      columns.add(columnInfo);
    }
    return columns;
  }

  private String convertArrowTypeToUnityType(ArrowType arrowType) {
    if (arrowType instanceof ArrowType.Utf8) {
      return "STRING";
    } else if (arrowType instanceof ArrowType.Int) {
      ArrowType.Int intType = (ArrowType.Int) arrowType;
      if (intType.getBitWidth() == 32) {
        return "INT";
      } else if (intType.getBitWidth() == 64) {
        return "BIGINT";
      }
    } else if (arrowType instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
      if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
        return "FLOAT";
      } else if (fpType.getPrecision() == FloatingPointPrecision.DOUBLE) {
        return "DOUBLE";
      }
    } else if (arrowType instanceof ArrowType.Bool) {
      return "BOOLEAN";
    } else if (arrowType instanceof ArrowType.Date) {
      return "DATE";
    } else if (arrowType instanceof ArrowType.Timestamp) {
      return "TIMESTAMP";
    }
    return "STRING";
  }

  private String convertArrowTypeToUnityTypeJson(ArrowType arrowType) {
    if (arrowType instanceof ArrowType.Utf8) {
      return "{\"type\":\"string\"}";
    } else if (arrowType instanceof ArrowType.Int) {
      ArrowType.Int intType = (ArrowType.Int) arrowType;
      if (intType.getBitWidth() == 32) {
        return "{\"type\":\"integer\"}";
      } else if (intType.getBitWidth() == 64) {
        return "{\"type\":\"long\"}";
      }
    } else if (arrowType instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
      if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
        return "{\"type\":\"float\"}";
      } else if (fpType.getPrecision() == FloatingPointPrecision.DOUBLE) {
        return "{\"type\":\"double\"}";
      }
    } else if (arrowType instanceof ArrowType.Bool) {
      return "{\"type\":\"boolean\"}";
    } else if (arrowType instanceof ArrowType.Date) {
      return "{\"type\":\"date\"}";
    } else if (arrowType instanceof ArrowType.Timestamp) {
      return "{\"type\":\"timestamp\"}";
    }
    return "{\"type\":\"string\"}";
  }
}
