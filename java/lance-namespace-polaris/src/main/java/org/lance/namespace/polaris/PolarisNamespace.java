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
package org.lance.namespace.polaris;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Polaris Catalog namespace implementation for Lance. */
public class PolarisNamespace implements LanceNamespace, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisNamespace.class);
  private static final String TABLE_FORMAT_LANCE = "lance";
  private static final String TABLE_TYPE_KEY = "table_type";

  private PolarisNamespaceConfig config;
  private RestClient restClient;
  private BufferAllocator allocator;

  public PolarisNamespace() {}

  @Override
  public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
    this.allocator = allocator;
    this.config = new PolarisNamespaceConfig(configProperties);

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
    LOG.info("Initialized Polaris namespace with endpoint: {}", config.getEndpoint());
  }

  @Override
  public String namespaceId() {
    String endpoint = this.config.getEndpoint();
    return String.format("PolarisNamespace { endpoint: \"%s\" }", endpoint);
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    ObjectIdentifier namespaceId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        namespaceId.levels() >= 1, "Namespace must have at least one level");

    try {
      List<String> namespace = namespaceId.listStyleId();

      PolarisModels.CreateNamespaceRequest polarisRequest =
          new PolarisModels.CreateNamespaceRequest(namespace, request.getProperties());

      PolarisModels.NamespaceResponse response =
          restClient.post("/namespaces", polarisRequest, PolarisModels.NamespaceResponse.class);

      LOG.info("Created namespace: {}", String.join(".", namespace));

      CreateNamespaceResponse result = new CreateNamespaceResponse();
      result.setProperties(response.getProperties());
      return result;
    } catch (RestClientException e) {
      if (e.isConflict()) {
        throw new NamespaceAlreadyExistsException(
            "Namespace already exists: " + namespaceId.stringStyleId());
      }
      throw new InternalException("Failed to create namespace: " + e.getMessage());
    }
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
    ObjectIdentifier namespaceId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        namespaceId.levels() >= 1, "Namespace must have at least one level");

    try {
      String namespacePath = namespaceId.stringStyleId();

      PolarisModels.NamespaceResponse response =
          restClient.get("/namespaces/" + namespacePath, PolarisModels.NamespaceResponse.class);

      DescribeNamespaceResponse result = new DescribeNamespaceResponse();
      result.setProperties(response.getProperties());
      return result;
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new NamespaceNotFoundException("Namespace not found: " + namespaceId.stringStyleId());
      }
      throw new InternalException("Failed to describe namespace: " + e.getMessage());
    }
  }

  @Override
  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    ObjectIdentifier parentId =
        request.getId() != null
            ? ObjectIdentifier.of(request.getId())
            : ObjectIdentifier.of(Collections.emptyList());

    try {
      String path = "/namespaces";
      if (!parentId.isRoot()) {
        path += "/" + parentId.stringStyleId() + "/namespaces";
      }

      PolarisModels.ListNamespacesResponse response =
          restClient.get(path, PolarisModels.ListNamespacesResponse.class);

      ListNamespacesResponse result = new ListNamespacesResponse();
      Set<String> namespaceSet = new LinkedHashSet<>();
      if (response.getNamespaces() != null) {
        for (PolarisModels.ListNamespacesResponse.Namespace ns : response.getNamespaces()) {
          namespaceSet.add(String.join(".", ns.getNamespace()));
        }
      }
      result.setNamespaces(namespaceSet);
      return result;
    } catch (RestClientException e) {
      throw new InternalException("Failed to list namespaces: " + e.getMessage());
    }
  }

  @Override
  public DropNamespaceResponse dropNamespace(DropNamespaceRequest request) {
    ObjectIdentifier namespaceId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        namespaceId.levels() >= 1, "Namespace must have at least one level");

    try {
      String namespacePath = namespaceId.stringStyleId();
      restClient.delete("/namespaces/" + namespacePath);
      LOG.info("Dropped namespace: {}", namespacePath);
      return new DropNamespaceResponse();
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        return new DropNamespaceResponse();
      }
      throw new InternalException("Failed to drop namespace: " + e.getMessage());
    }
  }

  @Override
  public void namespaceExists(NamespaceExistsRequest request) {
    ObjectIdentifier namespaceId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        namespaceId.levels() >= 1, "Namespace must have at least one level");

    try {
      String namespacePath = namespaceId.stringStyleId();
      restClient.get("/namespaces/" + namespacePath, PolarisModels.NamespaceResponse.class);
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new NamespaceNotFoundException("Namespace not found: " + namespaceId.stringStyleId());
      }
      throw new InternalException("Failed to check namespace existence: " + e.getMessage());
    }
  }

  @Override
  public void tableExists(TableExistsRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() >= 2, "Table identifier must have at least 2 levels");

    try {
      List<String> parts = tableId.listStyleId();
      String tableName = parts.get(parts.size() - 1);
      List<String> namespaceParts = parts.subList(0, parts.size() - 1);
      String namespacePath = String.join(".", namespaceParts);

      restClient.get(
          "/namespaces/" + namespacePath + "/generic-tables/" + tableName,
          PolarisModels.LoadGenericTableResponse.class);
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new TableNotFoundException("Table not found: " + tableId.stringStyleId());
      }
      throw new InternalException("Failed to check table existence: " + e.getMessage());
    }
  }

  @Override
  public CreateEmptyTableResponse createEmptyTable(CreateEmptyTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() >= 2, "Table identifier must have at least 2 levels");

    try {
      List<String> parts = tableId.listStyleId();
      String tableName = parts.get(parts.size() - 1);
      List<String> namespaceParts = parts.subList(0, parts.size() - 1);
      String namespacePath = String.join(".", namespaceParts);

      Map<String, String> properties = new HashMap<>();
      properties.put(TABLE_TYPE_KEY, TABLE_FORMAT_LANCE);
      String comment = null;

      PolarisModels.CreateGenericTableRequest tableRequest =
          new PolarisModels.CreateGenericTableRequest(
              tableName, TABLE_FORMAT_LANCE, request.getLocation(), comment, properties);

      PolarisModels.LoadGenericTableResponse response =
          restClient.post(
              "/namespaces/" + namespacePath + "/generic-tables",
              tableRequest,
              PolarisModels.LoadGenericTableResponse.class);

      LOG.info("Created Lance table: {}.{}", namespacePath, tableName);

      CreateEmptyTableResponse result = new CreateEmptyTableResponse();
      result.setLocation(response.getTable().getBaseLocation());
      return result;
    } catch (RestClientException e) {
      if (e.isConflict()) {
        throw new TableAlreadyExistsException("Table already exists: " + tableId.stringStyleId());
      }
      throw new InternalException("Failed to create table: " + e.getMessage());
    }
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() >= 2, "Table identifier must have at least 2 levels");

    try {
      List<String> parts = tableId.listStyleId();
      String tableName = parts.get(parts.size() - 1);
      List<String> namespaceParts = parts.subList(0, parts.size() - 1);
      String namespacePath = String.join(".", namespaceParts);

      PolarisModels.LoadGenericTableResponse response =
          restClient.get(
              "/namespaces/" + namespacePath + "/generic-tables/" + tableName,
              PolarisModels.LoadGenericTableResponse.class);

      PolarisModels.GenericTable table = response.getTable();

      if (!TABLE_FORMAT_LANCE.equals(table.getFormat())) {
        throw new InvalidInputException(
            String.format(
                "Table %s is not a Lance table (format: %s)",
                tableId.stringStyleId(), table.getFormat()));
      }

      DescribeTableResponse result = new DescribeTableResponse();
      result.setLocation(table.getBaseLocation());
      result.setStorageOptions(table.getProperties());
      return result;
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new TableNotFoundException("Table not found: " + tableId.stringStyleId());
      }
      throw new InternalException("Failed to describe table: " + e.getMessage());
    }
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    ObjectIdentifier namespaceId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        namespaceId.levels() >= 1, "Namespace must have at least one level");

    try {
      String namespacePath = namespaceId.stringStyleId();

      PolarisModels.ListGenericTablesResponse response =
          restClient.get(
              "/namespaces/" + namespacePath + "/generic-tables",
              PolarisModels.ListGenericTablesResponse.class);

      ListTablesResponse result = new ListTablesResponse();
      Set<String> tableNames = new LinkedHashSet<>();
      if (response.getIdentifiers() != null) {
        for (PolarisModels.TableIdentifier id : response.getIdentifiers()) {
          tableNames.add(id.getName());
        }
      }
      result.setTables(tableNames);
      return result;
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new NamespaceNotFoundException("Namespace not found: " + namespaceId.stringStyleId());
      }
      throw new InternalException("Failed to list tables: " + e.getMessage());
    }
  }

  @Override
  public DeregisterTableResponse deregisterTable(DeregisterTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());
    ValidationUtil.checkArgument(
        tableId.levels() >= 2, "Table identifier must have at least 2 levels");

    try {
      List<String> parts = tableId.listStyleId();
      String tableName = parts.get(parts.size() - 1);
      List<String> namespaceParts = parts.subList(0, parts.size() - 1);
      String namespacePath = String.join(".", namespaceParts);

      PolarisModels.LoadGenericTableResponse getResponse =
          restClient.get(
              "/namespaces/" + namespacePath + "/generic-tables/" + tableName,
              PolarisModels.LoadGenericTableResponse.class);

      String location = getResponse.getTable().getBaseLocation();
      restClient.delete("/namespaces/" + namespacePath + "/generic-tables/" + tableName);

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
}
