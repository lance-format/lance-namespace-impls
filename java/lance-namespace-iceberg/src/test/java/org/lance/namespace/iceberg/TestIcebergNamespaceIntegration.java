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

import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.TableExistsRequest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for IcebergNamespace against a running Iceberg REST Catalog.
 *
 * <p>This test uses Lakekeeper as the Iceberg REST Catalog implementation. To run these tests,
 * start the catalog with:
 *
 * <pre>
 *   cd docker/iceberg && docker-compose up -d
 * </pre>
 *
 * <p>Tests are automatically skipped if the catalog is not available.
 */
public class TestIcebergNamespaceIntegration {

  private static final String ICEBERG_ENDPOINT = "http://localhost:8282/catalog";
  private static final String TEST_WAREHOUSE = "test_warehouse";
  private static boolean icebergAvailable = false;

  private IcebergNamespace namespace;
  private BufferAllocator allocator;
  private String testNamespace;

  @BeforeAll
  public static void checkIcebergAvailable() {
    try {
      URL url = new URL(ICEBERG_ENDPOINT + "/v1/config?warehouse=" + TEST_WAREHOUSE);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);

      int responseCode = conn.getResponseCode();
      conn.disconnect();

      icebergAvailable = responseCode == 200;

      if (!icebergAvailable) {
        System.out.println(
            "Iceberg REST Catalog is not available at "
                + ICEBERG_ENDPOINT
                + " - skipping integration tests");
      } else {
        System.out.println(
            "Iceberg REST Catalog detected at "
                + ICEBERG_ENDPOINT
                + " (response code: "
                + responseCode
                + ")");
      }
    } catch (Exception e) {
      icebergAvailable = false;
      System.out.println(
          "Iceberg REST Catalog is not available at "
              + ICEBERG_ENDPOINT
              + " ("
              + e.getMessage()
              + ") - skipping integration tests");
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    Assumptions.assumeTrue(
        icebergAvailable, "Iceberg REST Catalog is not available at " + ICEBERG_ENDPOINT);

    allocator = new RootAllocator();
    namespace = new IcebergNamespace();

    String uniqueId = UUID.randomUUID().toString().substring(0, 8);
    testNamespace = "test_ns_" + uniqueId;

    Map<String, String> config = new HashMap<>();
    config.put("endpoint", ICEBERG_ENDPOINT);
    config.put("root", "s3://warehouse");

    namespace.initialize(config, allocator);
  }

  @AfterEach
  public void tearDown() {
    try {
      DropNamespaceRequest dropRequest = new DropNamespaceRequest();
      dropRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace));
      namespace.dropNamespace(dropRequest);
    } catch (Exception e) {
      // Ignore cleanup errors
    }

    if (allocator != null) {
      allocator.close();
    }
  }

  @Test
  public void testNamespaceOperations() {
    // Create namespace
    CreateNamespaceRequest createRequest = new CreateNamespaceRequest();
    createRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace));
    createRequest.setProperties(Collections.singletonMap("description", "Test namespace"));

    CreateNamespaceResponse createResponse = namespace.createNamespace(createRequest);
    assertThat(createResponse).isNotNull();

    // Describe namespace
    DescribeNamespaceRequest describeRequest = new DescribeNamespaceRequest();
    describeRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace));

    DescribeNamespaceResponse describeResponse = namespace.describeNamespace(describeRequest);
    assertThat(describeResponse).isNotNull();

    // Check namespace exists
    NamespaceExistsRequest existsRequest = new NamespaceExistsRequest();
    existsRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace));
    namespace.namespaceExists(existsRequest);

    // List namespaces
    ListNamespacesRequest listRequest = new ListNamespacesRequest();
    listRequest.setId(Collections.singletonList(TEST_WAREHOUSE));
    ListNamespacesResponse listResponse = namespace.listNamespaces(listRequest);
    assertThat(listResponse.getNamespaces()).contains(TEST_WAREHOUSE + "." + testNamespace);

    // Drop namespace
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace));
    namespace.dropNamespace(dropRequest);

    // Verify namespace doesn't exist
    assertThatThrownBy(() -> namespace.namespaceExists(existsRequest))
        .isInstanceOf(LanceNamespaceException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void testTableOperations() {
    // Create namespace first
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace));
    namespace.createNamespace(nsRequest);

    String tableName = "test_table_" + UUID.randomUUID().toString().substring(0, 8);

    // Declare table
    DeclareTableRequest createRequest = new DeclareTableRequest();
    createRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace, tableName));
    createRequest.setLocation("s3://warehouse/" + testNamespace + "/" + tableName);

    DeclareTableResponse createResponse = namespace.declareTable(createRequest);
    assertThat(createResponse.getLocation()).isNotNull();

    // Describe table
    DescribeTableRequest describeRequest = new DescribeTableRequest();
    describeRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace, tableName));

    DescribeTableResponse describeResponse = namespace.describeTable(describeRequest);
    assertThat(describeResponse.getLocation()).isNotNull();

    // Check table exists
    TableExistsRequest existsRequest = new TableExistsRequest();
    existsRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace, tableName));
    namespace.tableExists(existsRequest);

    // List tables
    ListTablesRequest listRequest = new ListTablesRequest();
    listRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace));

    ListTablesResponse listResponse = namespace.listTables(listRequest);
    assertThat(listResponse.getTables()).contains(tableName);

    // Deregister table
    DeregisterTableRequest deregisterRequest = new DeregisterTableRequest();
    deregisterRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace, tableName));
    namespace.deregisterTable(deregisterRequest);

    // Verify table doesn't exist
    assertThatThrownBy(() -> namespace.tableExists(existsRequest))
        .isInstanceOf(LanceNamespaceException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void testDeclareTableWithLocation() {
    // Create namespace first
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace));
    namespace.createNamespace(nsRequest);

    String tableName = "lance_table";
    DeclareTableRequest createRequest = new DeclareTableRequest();
    createRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace, tableName));
    createRequest.setLocation("s3://warehouse/" + testNamespace + "/" + tableName);

    DeclareTableResponse response = namespace.declareTable(createRequest);
    assertThat(response.getLocation()).isNotNull();

    // Clean up table
    DeregisterTableRequest deregisterRequest = new DeregisterTableRequest();
    deregisterRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace, tableName));
    namespace.deregisterTable(deregisterRequest);
  }

  @Test
  public void testNestedNamespace() {
    String nestedNs = "nested_" + UUID.randomUUID().toString().substring(0, 8);

    // Create parent namespace
    CreateNamespaceRequest parentRequest = new CreateNamespaceRequest();
    parentRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace));
    namespace.createNamespace(parentRequest);

    // Create nested namespace
    CreateNamespaceRequest nestedRequest = new CreateNamespaceRequest();
    nestedRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace, nestedNs));
    nestedRequest.setProperties(Collections.singletonMap("description", "Nested namespace"));
    namespace.createNamespace(nestedRequest);

    // List nested namespaces
    ListNamespacesRequest listRequest = new ListNamespacesRequest();
    listRequest.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace));
    ListNamespacesResponse listResponse = namespace.listNamespaces(listRequest);
    assertThat(listResponse.getNamespaces())
        .contains(TEST_WAREHOUSE + "." + testNamespace + "." + nestedNs);

    // Drop nested namespace first
    DropNamespaceRequest dropNested = new DropNamespaceRequest();
    dropNested.setId(Arrays.asList(TEST_WAREHOUSE, testNamespace, nestedNs));
    namespace.dropNamespace(dropNested);
  }
}
