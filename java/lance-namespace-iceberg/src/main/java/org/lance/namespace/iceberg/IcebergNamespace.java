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
package org.lance.namespace.iceberg;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.LanceNamespaceException;
import org.lance.namespace.ObjectIdentifier;
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
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.DropTableResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.TableExistsRequest;
import org.lance.namespace.rest.RestClient;
import org.lance.namespace.util.ValidationUtil;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Iceberg REST Catalog namespace implementation for Lance. */
public class IcebergNamespace implements LanceNamespace {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergNamespace.class);
  private static final String TABLE_TYPE_LANCE = "lance";
  private static final String TABLE_TYPE_KEY = "table_type";
  private static final char NAMESPACE_SEPARATOR = '\u001F';

  private IcebergNamespaceConfig config;
  private RestClient restClient;
  private BufferAllocator allocator;

  public IcebergNamespace() {}

  @Override
  public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
    this.allocator = allocator;
    this.config = new IcebergNamespaceConfig(configProperties);

    RestClient.Builder clientBuilder =
        RestClient.builder()
            .baseUrl(config.getFullApiUrl())
            .connectTimeout(config.getConnectTimeout())
            .readTimeout(config.getReadTimeout())
            .maxRetries(config.getMaxRetries());

    Map<String, String> headers = new HashMap<>();
    if (config.getAuthToken() != null) {
      headers.put("Authorization", "Bearer " + config.getAuthToken());
    }
    if (config.getWarehouse() != null) {
      headers.put("X-Iceberg-Access-Delegation", "vended-credentials");
    }
    if (!headers.isEmpty()) {
      clientBuilder.defaultHeaders(headers);
    }

    this.restClient = clientBuilder.build();
    LOG.info("Initialized Iceberg namespace with endpoint: {}", config.getEndpoint());
  }

  @Override
  public String namespaceId() {
    return String.format("IcebergNamespace { endpoint: \"%s\" }", config.getEndpoint());
  }

  @Override
  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());

    try {
      Map<String, String> params = new HashMap<>();
      if (nsId.levels() > 0) {
        String parent = encodeNamespace(nsId.getIdentifier());
        params.put("parent", parent);
      }
      if (request.getPageToken() != null) {
        params.put("pageToken", request.getPageToken());
      }

      IcebergModels.ListNamespacesResponse response =
          restClient.get("/namespaces", params, IcebergModels.ListNamespacesResponse.class);

      List<String> namespaces = new ArrayList<>();
      if (response != null && response.getNamespaces() != null) {
        for (List<String> ns : response.getNamespaces()) {
          if (!ns.isEmpty()) {
            namespaces.add(ns.get(ns.size() - 1));
          }
        }
      }

      Collections.sort(namespaces);
      Set<String> resultNamespaces = new LinkedHashSet<>(namespaces);

      ListNamespacesResponse result = new ListNamespacesResponse();
      result.setNamespaces(resultNamespaces);
      return result;

    } catch (IOException e) {
      throw new LanceNamespaceException(500, "Failed to list namespaces: " + e.getMessage());
    }
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(nsId.levels() >= 1, "Namespace must have at least one level");

    try {
      IcebergModels.CreateNamespaceRequest createRequest =
          new IcebergModels.CreateNamespaceRequest();
      createRequest.setNamespace(nsId.getIdentifier());
      createRequest.setProperties(request.getProperties());

      IcebergModels.CreateNamespaceResponse response =
          restClient.post("/namespaces", createRequest, IcebergModels.CreateNamespaceResponse.class);

      CreateNamespaceResponse result = new CreateNamespaceResponse();
      result.setProperties(response != null ? response.getProperties() : null);
      return result;

    } catch (RestClient.RestClientException e) {
      if (e.getStatusCode() == 409) {
        throw LanceNamespaceException.conflict(
            "Namespace already exists",
            "NAMESPACE_EXISTS",
            request.getId().toString(),
            e.getResponseBody());
      }
      throw new LanceNamespaceException(500, "Failed to create namespace: " + e.getMessage());
    } catch (IOException e) {
      throw new LanceNamespaceException(500, "Failed to create namespace: " + e.getMessage());
    }
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(nsId.levels() >= 1, "Namespace must have at least one level");

    try {
      String namespacePath = encodeNamespace(nsId.getIdentifier());
      IcebergModels.GetNamespaceResponse response =
          restClient.get("/namespaces/" + namespacePath, IcebergModels.GetNamespaceResponse.class);

      DescribeNamespaceResponse result = new DescribeNamespaceResponse();
      result.setProperties(response != null ? response.getProperties() : null);
      return result;

    } catch (RestClient.RestClientException e) {
      if (e.getStatusCode() == 404) {
        throw LanceNamespaceException.notFound(
            "Namespace not found",
            "NAMESPACE_NOT_FOUND",
            request.getId().toString(),
            e.getResponseBody());
      }
      throw new LanceNamespaceException(500, "Failed to describe namespace: " + e.getMessage());
    } catch (IOException e) {
      throw new LanceNamespaceException(500, "Failed to describe namespace: " + e.getMessage());
    }
  }

  @Override
  public void namespaceExists(NamespaceExistsRequest request) {
    describeNamespace(new DescribeNamespaceRequest().id(request.getId()));
  }

  @Override
  public DropNamespaceResponse dropNamespace(DropNamespaceRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(nsId.levels() >= 1, "Namespace must have at least one level");

    try {
      String namespacePath = encodeNamespace(nsId.getIdentifier());
      restClient.delete("/namespaces/" + namespacePath);

      return new DropNamespaceResponse();

    } catch (RestClient.RestClientException e) {
      if (e.getStatusCode() == 404) {
        return new DropNamespaceResponse();
      }
      if (e.getStatusCode() == 409) {
        throw LanceNamespaceException.conflict(
            "Namespace not empty",
            "NAMESPACE_NOT_EMPTY",
            request.getId().toString(),
            e.getResponseBody());
      }
      throw new LanceNamespaceException(500, "Failed to drop namespace: " + e.getMessage());
    } catch (IOException e) {
      throw new LanceNamespaceException(500, "Failed to drop namespace: " + e.getMessage());
    }
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(nsId.levels() >= 1, "Namespace must have at least one level");

    try {
      String namespacePath = encodeNamespace(nsId.getIdentifier());
      Map<String, String> params = new HashMap<>();
      if (request.getPageToken() != null) {
        params.put("pageToken", request.getPageToken());
      }

      IcebergModels.ListTablesResponse response =
          restClient.get(
              "/namespaces/" + namespacePath + "/tables",
              params,
              IcebergModels.ListTablesResponse.class);

      List<String> tables = new ArrayList<>();
      if (response != null && response.getIdentifiers() != null) {
        for (IcebergModels.TableIdentifier tableId : response.getIdentifiers()) {
          if (isLanceTable(nsId.getIdentifier(), tableId.getName())) {
            tables.add(tableId.getName());
          }
        }
      }

      Collections.sort(tables);
      Set<String> resultTables = new LinkedHashSet<>(tables);

      ListTablesResponse result = new ListTablesResponse();
      result.setTables(resultTables);
      return result;

    } catch (IOException e) {
      throw new LanceNamespaceException(500, "Failed to list tables: " + e.getMessage());
    }
  }

  @Override
  public CreateEmptyTableResponse createEmptyTable(CreateEmptyTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() >= 2, "Table identifier must have at least namespace and table name");

    List<String> namespace = tableId.getIdentifier().subList(0, tableId.levels() - 1);
    String tableName = tableId.levelAtListPos(tableId.levels() - 1);

    try {
      String tablePath = request.getLocation();
      if (tablePath == null || tablePath.isEmpty()) {
        tablePath = config.getRoot() + "/" + String.join("/", namespace) + "/" + tableName;
      }

      IcebergModels.CreateTableRequest createRequest = new IcebergModels.CreateTableRequest();
      createRequest.setName(tableName);
      createRequest.setLocation(tablePath);
      createRequest.setSchema(IcebergModels.createDummySchema());

      Map<String, String> properties = new HashMap<>();
      properties.put(TABLE_TYPE_KEY, TABLE_TYPE_LANCE);
      if (request.getProperties() != null) {
        properties.putAll(request.getProperties());
      }
      createRequest.setProperties(properties);

      String namespacePath = encodeNamespace(namespace);
      IcebergModels.LoadTableResponse response =
          restClient.post(
              "/namespaces/" + namespacePath + "/tables",
              createRequest,
              IcebergModels.LoadTableResponse.class);

      CreateEmptyTableResponse result = new CreateEmptyTableResponse();
      result.setLocation(tablePath);
      if (response != null && response.getMetadata() != null) {
        result.setProperties(response.getMetadata().getProperties());
      }
      return result;

    } catch (RestClient.RestClientException e) {
      if (e.getStatusCode() == 409) {
        throw LanceNamespaceException.conflict(
            "Table already exists",
            "TABLE_EXISTS",
            request.getId().toString(),
            e.getResponseBody());
      }
      if (e.getStatusCode() == 404) {
        throw LanceNamespaceException.notFound(
            "Namespace not found",
            "NAMESPACE_NOT_FOUND",
            String.join(".", namespace),
            e.getResponseBody());
      }
      throw new LanceNamespaceException(500, "Failed to create empty table: " + e.getMessage());
    } catch (IOException e) {
      throw new LanceNamespaceException(500, "Failed to create empty table: " + e.getMessage());
    }
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() >= 2, "Table identifier must have at least namespace and table name");

    List<String> namespace = tableId.getIdentifier().subList(0, tableId.levels() - 1);
    String tableName = tableId.levelAtListPos(tableId.levels() - 1);

    try {
      String namespacePath = encodeNamespace(namespace);
      String encodedTableName = URLEncoder.encode(tableName, StandardCharsets.UTF_8);

      IcebergModels.LoadTableResponse response =
          restClient.get(
              "/namespaces/" + namespacePath + "/tables/" + encodedTableName,
              IcebergModels.LoadTableResponse.class);

      if (response == null || response.getMetadata() == null) {
        throw LanceNamespaceException.notFound(
            "Table not found", "TABLE_NOT_FOUND", request.getId().toString(), "No metadata");
      }

      Map<String, String> props = response.getMetadata().getProperties();
      if (props == null || !TABLE_TYPE_LANCE.equalsIgnoreCase(props.get(TABLE_TYPE_KEY))) {
        throw LanceNamespaceException.badRequest(
            "Not a Lance table",
            "INVALID_TABLE",
            request.getId().toString(),
            "Table is not managed by Lance");
      }

      DescribeTableResponse result = new DescribeTableResponse();
      result.setLocation(response.getMetadata().getLocation());
      result.setProperties(props);
      return result;

    } catch (RestClient.RestClientException e) {
      if (e.getStatusCode() == 404) {
        throw LanceNamespaceException.notFound(
            "Table not found", "TABLE_NOT_FOUND", request.getId().toString(), e.getResponseBody());
      }
      throw new LanceNamespaceException(500, "Failed to describe table: " + e.getMessage());
    } catch (IOException e) {
      throw new LanceNamespaceException(500, "Failed to describe table: " + e.getMessage());
    }
  }

  @Override
  public void tableExists(TableExistsRequest request) {
    describeTable(new DescribeTableRequest().id(request.getId()));
  }

  @Override
  public DropTableResponse dropTable(DropTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() >= 2, "Table identifier must have at least namespace and table name");

    List<String> namespace = tableId.getIdentifier().subList(0, tableId.levels() - 1);
    String tableName = tableId.levelAtListPos(tableId.levels() - 1);

    try {
      String namespacePath = encodeNamespace(namespace);
      String encodedTableName = URLEncoder.encode(tableName, StandardCharsets.UTF_8);

      String tableLocation = null;
      try {
        IcebergModels.LoadTableResponse tableResponse =
            restClient.get(
                "/namespaces/" + namespacePath + "/tables/" + encodedTableName,
                IcebergModels.LoadTableResponse.class);
        if (tableResponse != null && tableResponse.getMetadata() != null) {
          tableLocation = tableResponse.getMetadata().getLocation();
        }
      } catch (RestClient.RestClientException e) {
        if (e.getStatusCode() == 404) {
          DropTableResponse result = new DropTableResponse();
          result.setId(request.getId());
          return result;
        }
      }

      Map<String, String> params = new HashMap<>();
      params.put("purgeRequested", "false");
      restClient.delete("/namespaces/" + namespacePath + "/tables/" + encodedTableName, params);

      DropTableResponse result = new DropTableResponse();
      result.setId(request.getId());
      result.setLocation(tableLocation);
      return result;

    } catch (IOException e) {
      throw new LanceNamespaceException(500, "Failed to drop table: " + e.getMessage());
    }
  }

  public void close() throws IOException {
    if (restClient != null) {
      restClient.close();
    }
  }

  private String encodeNamespace(List<String> namespace) {
    String joined =
        namespace.stream()
            .map(s -> URLEncoder.encode(s, StandardCharsets.UTF_8))
            .collect(Collectors.joining(String.valueOf(NAMESPACE_SEPARATOR)));
    return URLEncoder.encode(joined, StandardCharsets.UTF_8);
  }

  private boolean isLanceTable(List<String> namespace, String tableName) {
    try {
      String namespacePath = encodeNamespace(namespace);
      String encodedTableName = URLEncoder.encode(tableName, StandardCharsets.UTF_8);

      IcebergModels.LoadTableResponse response =
          restClient.get(
              "/namespaces/" + namespacePath + "/tables/" + encodedTableName,
              IcebergModels.LoadTableResponse.class);

      if (response != null && response.getMetadata() != null) {
        Map<String, String> props = response.getMetadata().getProperties();
        return props != null && TABLE_TYPE_LANCE.equalsIgnoreCase(props.get(TABLE_TYPE_KEY));
      }
    } catch (Exception e) {
      LOG.debug("Failed to check if table is Lance table: {}", e.getMessage());
    }
    return false;
  }
}
