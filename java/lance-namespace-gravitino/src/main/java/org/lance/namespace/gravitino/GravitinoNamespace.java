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
package org.lance.namespace.gravitino;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableAlreadyExistsException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Gravitino REST namespace implementation for Lance.
 *
 * <p>This implementation integrates with Apache Gravitino's Lance REST service to provide namespace
 * and table management capabilities. It follows Gravitino's three-level hierarchy: catalog → schema
 * → table.
 *
 * <p>Configuration properties:
 *
 * <ul>
 *   <li>endpoint: Gravitino server endpoint (e.g., "http://localhost:9101")
 *   <li>auth_token: Optional authentication token
 *   <li>connect_timeout: Connection timeout in milliseconds (default: 10000)
 *   <li>read_timeout: Read timeout in milliseconds (default: 30000)
 *   <li>max_retries: Maximum retry attempts (default: 3)
 * </ul>
 *
 * <p>Namespace ID format: [catalog, schema]
 *
 * <p>Table ID format: [catalog, schema, table_name]
 */
public class GravitinoNamespace implements LanceNamespace, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoNamespace.class);

  private GravitinoNamespaceConfig config;
  private RestClient restClient;
  private BufferAllocator allocator;

  public GravitinoNamespace() {}

  @Override
  public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
    this.allocator = allocator;
    this.config = new GravitinoNamespaceConfig(configProperties);

    RestClient.Builder clientBuilder =
        RestClient.builder()
            .baseUrl(config.getBaseApiUrl())
            .connectTimeout(config.getConnectTimeout(), TimeUnit.MILLISECONDS)
            .readTimeout(config.getReadTimeout(), TimeUnit.MILLISECONDS)
            .maxRetries(config.getMaxRetries());

    if (config.getAuthToken() != null) {
      clientBuilder.authToken(config.getAuthToken());
    }

    this.restClient = clientBuilder.build();
    LOG.info("Initialized Gravitino namespace with endpoint: {}", config.getEndpoint());
  }

  @Override
  public String namespaceId() {
    return String.format("GravitinoNamespace { endpoint: \"%s\" }", config.getEndpoint());
  }

  private String encodeNamespacePath(List<String> namespaceParts) {
    if (namespaceParts.isEmpty()) {
      return "";
    }
    // Gravitino uses $ as delimiter, URL-encoded as %24
    StringBuilder sb = new StringBuilder();
    try {
      for (int i = 0; i < namespaceParts.size(); i++) {
        if (i > 0) {
          sb.append("%24");
        }
        sb.append(URLEncoder.encode(namespaceParts.get(i), "UTF-8"));
      }
    } catch (UnsupportedEncodingException e) {
      throw new InternalException("Failed to encode namespace path: " + e.getMessage());
    }
    return sb.toString();
  }

  @Override
  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    ObjectIdentifier nsId =
        request.getId() != null
            ? ObjectIdentifier.of(request.getId())
            : ObjectIdentifier.of(Collections.emptyList());

    try {
      List<String> namespaces = new ArrayList<>();
      GravitinoModels.ListNamespacesResponse response;

      if (nsId.levels() == 0) {
        // List catalogs (top-level namespaces) using the correct endpoint
        Map<String, String> params = new HashMap<>();
        params.put("delimiter", ".");
        response =
            restClient.get(
                "/namespace/%2E/list", params, GravitinoModels.ListNamespacesResponse.class);
      } else if (nsId.levels() == 1) {
        // List schemas in catalog
        String catalog = nsId.levelAtListPos(0);
        String encodedCatalog;
        try {
          encodedCatalog = URLEncoder.encode(catalog, "UTF-8");
        } catch (UnsupportedEncodingException e) {
          throw new InternalException("Failed to encode catalog name: " + e.getMessage());
        }
        response =
            restClient.get(
                "/namespace/" + encodedCatalog + "/list",
                GravitinoModels.ListNamespacesResponse.class);
      } else {
        // Cannot list below schema level
        ListNamespacesResponse result = new ListNamespacesResponse();
        result.setNamespaces(Collections.emptySet());
        return result;
      }

      if (response != null && response.getNamespaces() != null) {
        for (String ns : response.getNamespaces()) {
          if (ns != null && !ns.isEmpty()) {
            if (nsId.levels() == 0) {
              // Top-level catalog
              namespaces.add(ns);
            } else {
              // Schema in catalog
              namespaces.add(nsId.levelAtListPos(0) + "." + ns);
            }
          }
        }
      }

      Collections.sort(namespaces);
      Set<String> resultNamespaces = new LinkedHashSet<>(namespaces);

      ListNamespacesResponse result = new ListNamespacesResponse();
      result.setNamespaces(resultNamespaces);
      return result;
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        ListNamespacesResponse result = new ListNamespacesResponse();
        result.setNamespaces(Collections.emptySet());
        return result;
      }
      throw new InternalException("Failed to list namespaces: " + e.getMessage());
    }
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(nsId.levels() > 0, "Namespace identifier cannot be empty");
    ValidationUtil.checkArgument(
        nsId.levels() <= 2, "Gravitino supports maximum 2-level namespaces (catalog.schema)");

    try {
      GravitinoModels.CreateNamespaceRequest createRequest =
          new GravitinoModels.CreateNamespaceRequest();
      createRequest.setId(nsId.listStyleId());
      createRequest.setMode("create");
      createRequest.setProperties(request.getProperties());

      String encodedPath = encodeNamespacePath(nsId.listStyleId());
      GravitinoModels.CreateNamespaceResponse response =
          restClient.post(
              "/namespace/" + encodedPath + "/create",
              createRequest,
              GravitinoModels.CreateNamespaceResponse.class);

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

    ValidationUtil.checkArgument(
        nsId.levels() > 0 && nsId.levels() <= 2,
        "Namespace must be 1 or 2 levels (catalog or catalog.schema)");

    try {
      GravitinoModels.DescribeNamespaceRequest describeRequest =
          new GravitinoModels.DescribeNamespaceRequest();
      describeRequest.setId(nsId.listStyleId());

      String encodedPath = encodeNamespacePath(nsId.listStyleId());
      GravitinoModels.DescribeNamespaceResponse response =
          restClient.post(
              "/namespace/" + encodedPath + "/describe",
              describeRequest,
              GravitinoModels.DescribeNamespaceResponse.class);

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

    ValidationUtil.checkArgument(
        nsId.levels() > 0 && nsId.levels() <= 2,
        "Namespace must be 1 or 2 levels (catalog or catalog.schema)");

    try {
      GravitinoModels.DropNamespaceRequest dropRequest = new GravitinoModels.DropNamespaceRequest();
      dropRequest.setId(nsId.listStyleId());

      // Include behavior if specified
      if (request.getBehavior() != null && !request.getBehavior().isEmpty()) {
        dropRequest.setBehavior(request.getBehavior());
      }

      String encodedPath = encodeNamespacePath(nsId.listStyleId());
      restClient.post("/namespace/" + encodedPath + "/drop", dropRequest, Void.class);

      LOG.info("Dropped namespace: {}", nsId.stringStyleId());
      return new DropNamespaceResponse();
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        return new DropNamespaceResponse();
      }
      if (e.isConflict()) {
        throw new InternalException("Namespace not empty: " + nsId.stringStyleId());
      }
      throw new InternalException("Failed to drop namespace: " + e.getMessage());
    }
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    ObjectIdentifier nsId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        nsId.levels() == 2, "Namespace must be exactly 2 levels (catalog.schema) to list tables");

    try {
      String encodedPath = encodeNamespacePath(nsId.listStyleId());
      GravitinoModels.ListTablesResponse response =
          restClient.get(
              "/namespace/" + encodedPath + "/table/list",
              GravitinoModels.ListTablesResponse.class);

      List<String> tables = new ArrayList<>();
      if (response != null && response.getTables() != null) {
        tables.addAll(response.getTables());
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
  public DeclareTableResponse declareTable(DeclareTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        tableId.levels() == 3, "Table identifier must be exactly 3 levels (catalog.schema.table)");

    try {
      GravitinoModels.RegisterTableRequest registerRequest =
          new GravitinoModels.RegisterTableRequest();
      registerRequest.setId(tableId.listStyleId());
      registerRequest.setLocation(request.getLocation());
      registerRequest.setMode("create");

      String encodedPath = encodeNamespacePath(tableId.listStyleId());
      GravitinoModels.RegisterTableResponse response =
          restClient.post(
              "/table/" + encodedPath + "/register",
              registerRequest,
              GravitinoModels.RegisterTableResponse.class);

      LOG.info("Declared table: {}", tableId.stringStyleId());

      DeclareTableResponse result = new DeclareTableResponse();
      result.setLocation(response != null ? response.getLocation() : request.getLocation());
      return result;
    } catch (RestClientException e) {
      if (e.isConflict()) {
        throw new TableAlreadyExistsException("Table already exists: " + tableId.stringStyleId());
      }
      if (e.isNotFound()) {
        List<String> nsId = tableId.listStyleId().subList(0, tableId.levels() - 1);
        throw new NamespaceNotFoundException("Namespace not found: " + String.join(".", nsId));
      }
      throw new InternalException("Failed to declare table: " + e.getMessage());
    }
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    if (Boolean.TRUE.equals(request.getLoadDetailedMetadata())) {
      throw new InvalidInputException(
          "load_detailed_metadata=true is not supported for this implementation");
    }

    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        tableId.levels() == 3, "Table identifier must be exactly 3 levels (catalog.schema.table)");

    try {
      GravitinoModels.TableExistsRequest existsRequest = new GravitinoModels.TableExistsRequest();
      existsRequest.setId(tableId.listStyleId());

      String encodedPath = encodeNamespacePath(tableId.listStyleId());
      GravitinoModels.TableExistsResponse existsResponse =
          restClient.post(
              "/table/" + encodedPath + "/exists",
              existsRequest,
              GravitinoModels.TableExistsResponse.class);

      if (existsResponse == null || !existsResponse.isExists()) {
        throw new TableNotFoundException("Table not found: " + tableId.stringStyleId());
      }

      // For Gravitino, we return basic information since there's no direct describe endpoint
      DescribeTableResponse result = new DescribeTableResponse();
      result.setLocation(null); // Location would need to be retrieved from table metadata
      result.setStorageOptions(Collections.emptyMap());
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
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        tableId.levels() == 3, "Table identifier must be exactly 3 levels (catalog.schema.table)");

    try {
      GravitinoModels.TableExistsRequest existsRequest = new GravitinoModels.TableExistsRequest();
      existsRequest.setId(tableId.listStyleId());

      String encodedPath = encodeNamespacePath(tableId.listStyleId());
      GravitinoModels.TableExistsResponse response =
          restClient.post(
              "/table/" + encodedPath + "/exists",
              existsRequest,
              GravitinoModels.TableExistsResponse.class);

      if (response == null || !response.isExists()) {
        throw new TableNotFoundException("Table not found: " + tableId.stringStyleId());
      }
    } catch (RestClientException e) {
      if (e.isNotFound()) {
        throw new TableNotFoundException("Table not found: " + tableId.stringStyleId());
      }
      throw new InternalException("Failed to check table existence: " + e.getMessage());
    }
  }

  @Override
  public DeregisterTableResponse deregisterTable(DeregisterTableRequest request) {
    ObjectIdentifier tableId = ObjectIdentifier.of(request.getId());

    ValidationUtil.checkArgument(
        tableId.levels() == 3, "Table identifier must be exactly 3 levels (catalog.schema.table)");

    try {
      GravitinoModels.DeregisterTableRequest deregisterRequest =
          new GravitinoModels.DeregisterTableRequest();
      deregisterRequest.setId(tableId.listStyleId());

      String encodedPath = encodeNamespacePath(tableId.listStyleId());
      GravitinoModels.DeregisterTableResponse response =
          restClient.post(
              "/table/" + encodedPath + "/deregister",
              deregisterRequest,
              GravitinoModels.DeregisterTableResponse.class);

      LOG.info("Deregistered table: {}", tableId.stringStyleId());

      DeregisterTableResponse result = new DeregisterTableResponse();
      result.setLocation(response != null ? response.getLocation() : null);
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
