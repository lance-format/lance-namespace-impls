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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Iceberg REST Catalog namespace implementation for Lance. */
public class IcebergNamespace implements LanceNamespace, Closeable {
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
            .connectTimeout(config.getConnectTimeout(), TimeUnit.MILLISECONDS)
            .readTimeout(config.getReadTimeout(), TimeUnit.MILLISECONDS)
            .maxRetries(config.getMaxRetries());

    if (config.getAuthToken() != null) {
      clientBuilder.authToken(config.getAuthToken());
    }
    if (config.getWarehouse() != null) {
      clientBuilder.header("X-Iceberg-Access-Delegation", "vended-credentials");
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
    ObjectIdentifier nsId =
        request.getId() != null
            ? ObjectIdentifier.of(request.getId())
            : ObjectIdentifier.of(Collections.emptyList());

    try {
      Map<String, String> params = new HashMap<>();
      if (nsId.levels() > 0) {
        String parent = encodeNamespace(nsId.listStyleId());
        params.put("parent", parent);
      }
      if (request.getPageToken() != null) {
        params.put("pageToken", request.getPageToken());
      }

      IcebergModels.ListNamespacesResponse response =
          params.isEmpty()
              ? restClient.get("/namespaces", IcebergModels.ListNamespacesResponse.class)
              : restClient.get("/namespaces", params, IcebergModels.ListNamespacesResponse.class);

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
    } catch (RestClientException e) {
      throw new InternalException("Failed to list namespaces: " + e.getMessage());
    }
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(nsId.levels() >= 1, "Namespace must have at least one level");

    try {
      IcebergModels.CreateNamespaceRequest createRequest =
          new IcebergModels.CreateNamespaceRequest();
      createRequest.setNamespace(nsId.listStyleId());
      createRequest.setProperties(request.getProperties());

      IcebergModels.CreateNamespaceResponse response =
          restClient.post(
              "/namespaces", createRequest, IcebergModels.CreateNamespaceResponse.class);

      LOG.info("Created namespace: {}", nsId.stringStyleId());

      CreateNamespaceResponse result = new CreateNamespaceResponse();
      result.setProperties(response != null ? response.getProperties() : null);
      return result;
    } catch (RestClientException e) {
      if (e.isConflict()) {
        throw new NamespaceAlreadyExistsException(
            "Namespace already exists: " + nsId.stringStyleId());
      }
      throw new InternalException("Failed to create namespace: " + e.getMessage());
    }
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(nsId.levels() >= 1, "Namespace must have at least one level");

    try {
      String namespacePath = encodeNamespace(nsId.listStyleId());
      IcebergModels.GetNamespaceResponse response =
          restClient.get("/namespaces/" + namespacePath, IcebergModels.GetNamespaceResponse.class);

      DescribeNamespaceResponse result = new DescribeNamespaceResponse();
      result.setProperties(response != null ? response.getProperties() : null);
      return result;
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new NamespaceNotFoundException("Namespace not found: " + nsId.stringStyleId());
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
    ValidationUtil.checkArgument(nsId.levels() >= 1, "Namespace must have at least one level");

    try {
      String namespacePath = encodeNamespace(nsId.listStyleId());
      restClient.delete("/namespaces/" + namespacePath);
      LOG.info("Dropped namespace: {}", nsId.stringStyleId());
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
    ValidationUtil.checkArgument(nsId.levels() >= 1, "Namespace must have at least one level");

    try {
      String namespacePath = encodeNamespace(nsId.listStyleId());
      Map<String, String> params = new HashMap<>();
      if (request.getPageToken() != null) {
        params.put("pageToken", request.getPageToken());
      }

      IcebergModels.ListTablesResponse response =
          params.isEmpty()
              ? restClient.get(
                  "/namespaces/" + namespacePath + "/tables",
                  IcebergModels.ListTablesResponse.class)
              : restClient.get(
                  "/namespaces/" + namespacePath + "/tables",
                  params,
                  IcebergModels.ListTablesResponse.class);

      List<String> tables = new ArrayList<>();
      if (response != null && response.getIdentifiers() != null) {
        for (IcebergModels.TableIdentifier tableId : response.getIdentifiers()) {
          if (isLanceTable(nsId.listStyleId(), tableId.getName())) {
            tables.add(tableId.getName());
          }
        }
      }

      Collections.sort(tables);
      Set<String> resultTables = new LinkedHashSet<>(tables);

      ListTablesResponse result = new ListTablesResponse();
      result.setTables(resultTables);
      return result;
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new NamespaceNotFoundException("Namespace not found: " + nsId.stringStyleId());
      }
      throw new InternalException("Failed to list tables: " + e.getMessage());
    }
  }

  @Override
  public CreateEmptyTableResponse createEmptyTable(CreateEmptyTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() >= 2, "Table identifier must have at least namespace and table name");

    List<String> namespace = tableId.listStyleId().subList(0, tableId.levels() - 1);
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
      createRequest.setProperties(properties);

      String namespacePath = encodeNamespace(namespace);
      IcebergModels.LoadTableResponse response =
          restClient.post(
              "/namespaces/" + namespacePath + "/tables",
              createRequest,
              IcebergModels.LoadTableResponse.class);

      LOG.info("Created Lance table: {}.{}", namespacePath, tableName);

      CreateEmptyTableResponse result = new CreateEmptyTableResponse();
      result.setLocation(tablePath);
      return result;
    } catch (RestClientException e) {
      if (e.isConflict()) {
        throw new TableAlreadyExistsException("Table already exists: " + tableId.stringStyleId());
      }
      if (e.isNotFound()) {
        throw new NamespaceNotFoundException("Namespace not found: " + String.join(".", namespace));
      }
      throw new InternalException("Failed to create table: " + e.getMessage());
    }
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() >= 2, "Table identifier must have at least namespace and table name");

    List<String> namespace = tableId.listStyleId().subList(0, tableId.levels() - 1);
    String tableName = tableId.levelAtListPos(tableId.levels() - 1);

    try {
      String namespacePath = encodeNamespace(namespace);
      String encodedTableName = urlEncode(tableName);

      IcebergModels.LoadTableResponse response =
          restClient.get(
              "/namespaces/" + namespacePath + "/tables/" + encodedTableName,
              IcebergModels.LoadTableResponse.class);

      if (response == null || response.getMetadata() == null) {
        throw new TableNotFoundException("Table not found: " + tableId.stringStyleId());
      }

      Map<String, String> props = response.getMetadata().getProperties();
      if (props == null || !TABLE_TYPE_LANCE.equalsIgnoreCase(props.get(TABLE_TYPE_KEY))) {
        throw new InvalidInputException(
            String.format(
                "Table %s is not a Lance table (missing table_type property)",
                tableId.stringStyleId()));
      }

      DescribeTableResponse result = new DescribeTableResponse();
      result.setLocation(response.getMetadata().getLocation());
      result.setStorageOptions(props);
      return result;
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new TableNotFoundException("Table not found: " + tableId.stringStyleId());
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
        tableId.levels() >= 2, "Table identifier must have at least namespace and table name");

    List<String> namespace = tableId.listStyleId().subList(0, tableId.levels() - 1);
    String tableName = tableId.levelAtListPos(tableId.levels() - 1);

    try {
      String namespacePath = encodeNamespace(namespace);
      String encodedTableName = urlEncode(tableName);

      IcebergModels.LoadTableResponse getResponse =
          restClient.get(
              "/namespaces/" + namespacePath + "/tables/" + encodedTableName,
              IcebergModels.LoadTableResponse.class);

      String location = null;
      if (getResponse != null && getResponse.getMetadata() != null) {
        location = getResponse.getMetadata().getLocation();
      }

      restClient.delete("/namespaces/" + namespacePath + "/tables/" + encodedTableName);
      LOG.info("Deregistered table: {}.{}", namespacePath, tableName);

      DeregisterTableResponse result = new DeregisterTableResponse();
      result.setLocation(location);
      return result;
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new TableNotFoundException("Table not found: " + tableId.stringStyleId());
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

  private String encodeNamespace(List<String> namespace) {
    String joined =
        namespace.stream()
            .map(this::urlEncode)
            .collect(Collectors.joining(String.valueOf(NAMESPACE_SEPARATOR)));
    return urlEncode(joined);
  }

  private String urlEncode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (java.io.UnsupportedEncodingException e) {
      throw new RuntimeException("UTF-8 encoding not supported", e);
    }
  }

  private boolean isLanceTable(List<String> namespace, String tableName) {
    try {
      String namespacePath = encodeNamespace(namespace);
      String encodedTableName = urlEncode(tableName);

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
