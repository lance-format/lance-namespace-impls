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

import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.CreateEmptyTableResponse;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
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
 * Integration tests for UnityNamespace against a running Unity Catalog.
 *
 * <p>To run these tests, start Unity Catalog with: cd docker && make up-unity
 *
 * <p>Default endpoint: http://localhost:8080
 *
 * <p>Tests are automatically skipped if Unity Catalog is not available.
 */
public class TestUnityNamespaceIntegration {

  private static final String UNITY_ENDPOINT = "http://localhost:8080";
  private static final String TEST_CATALOG = "lance_test";
  private static boolean unityAvailable = false;

  private UnityNamespace namespace;
  private BufferAllocator allocator;
  private String testSchema;

  @BeforeAll
  public static void checkUnityAvailable() {
    try {
      URL url = new URL(UNITY_ENDPOINT + "/api/2.1/unity-catalog/catalogs");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(2000);
      conn.setReadTimeout(2000);

      int responseCode = conn.getResponseCode();
      conn.disconnect();

      unityAvailable = responseCode == 200;

      if (unityAvailable) {
        System.out.println("Unity Catalog detected at " + UNITY_ENDPOINT);
        ensureTestCatalogExists();
      } else {
        System.out.println(
            "Unity Catalog is not available at "
                + UNITY_ENDPOINT
                + " (response: "
                + responseCode
                + ") - skipping integration tests");
      }
    } catch (Exception e) {
      unityAvailable = false;
      System.out.println(
          "Unity Catalog is not available at "
              + UNITY_ENDPOINT
              + " ("
              + e.getMessage()
              + ") - skipping integration tests");
    }
  }

  private static void ensureTestCatalogExists() {
    try {
      // Check if catalog exists
      URL checkUrl = new URL(UNITY_ENDPOINT + "/api/2.1/unity-catalog/catalogs/" + TEST_CATALOG);
      HttpURLConnection checkConn = (HttpURLConnection) checkUrl.openConnection();
      checkConn.setRequestMethod("GET");
      checkConn.setConnectTimeout(2000);

      if (checkConn.getResponseCode() == 404) {
        // Create catalog
        URL createUrl = new URL(UNITY_ENDPOINT + "/api/2.1/unity-catalog/catalogs");
        HttpURLConnection createConn = (HttpURLConnection) createUrl.openConnection();
        createConn.setRequestMethod("POST");
        createConn.setRequestProperty("Content-Type", "application/json");
        createConn.setDoOutput(true);

        String body =
            String.format("{\"name\": \"%s\", \"comment\": \"Test catalog\"}", TEST_CATALOG);
        createConn.getOutputStream().write(body.getBytes());

        int createResponse = createConn.getResponseCode();
        if (createResponse == 200 || createResponse == 201) {
          System.out.println("Created test catalog: " + TEST_CATALOG);
        } else {
          System.out.println("Failed to create catalog, response: " + createResponse);
        }
        createConn.disconnect();
      }
      checkConn.disconnect();
    } catch (Exception e) {
      System.out.println("Failed to ensure test catalog exists: " + e.getMessage());
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    Assumptions.assumeTrue(unityAvailable, "Unity Catalog is not available");

    allocator = new RootAllocator();
    namespace = new UnityNamespace();

    String uniqueId = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    testSchema = "test_schema_" + uniqueId;

    Map<String, String> config = new HashMap<>();
    config.put("endpoint", UNITY_ENDPOINT);
    config.put("catalog", TEST_CATALOG);
    config.put("root", "/tmp/lance-integration-test");

    namespace.initialize(config, allocator);
  }

  @AfterEach
  public void tearDown() {
    try {
      // Clean up test schema
      DropNamespaceRequest dropRequest = new DropNamespaceRequest();
      dropRequest.setId(Arrays.asList(TEST_CATALOG, testSchema));
      dropRequest.setBehavior("Restrict");
      namespace.dropNamespace(dropRequest);
    } catch (Exception e) {
      // Ignore cleanup errors
    }

    if (namespace != null) {
      try {
        namespace.close();
      } catch (Exception e) {
        // Ignore
      }
    }

    if (allocator != null) {
      allocator.close();
    }
  }

  @Test
  public void testListCatalog() {
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setId(Collections.emptyList());

    ListNamespacesResponse response = namespace.listNamespaces(request);
    assertThat(response.getNamespaces()).contains(TEST_CATALOG);
  }

  @Test
  public void testSchemaOperations() {
    // Create schema
    CreateNamespaceRequest createRequest = new CreateNamespaceRequest();
    createRequest.setId(Arrays.asList(TEST_CATALOG, testSchema));
    Map<String, String> props = new HashMap<>();
    props.put("comment", "Integration test schema");
    createRequest.setProperties(props);

    CreateNamespaceResponse createResponse = namespace.createNamespace(createRequest);
    assertThat(createResponse).isNotNull();

    // Describe schema
    DescribeNamespaceRequest describeRequest = new DescribeNamespaceRequest();
    describeRequest.setId(Arrays.asList(TEST_CATALOG, testSchema));

    DescribeNamespaceResponse describeResponse = namespace.describeNamespace(describeRequest);
    assertThat(describeResponse).isNotNull();

    // List schemas in catalog
    ListNamespacesRequest listRequest = new ListNamespacesRequest();
    listRequest.setId(Collections.singletonList(TEST_CATALOG));

    ListNamespacesResponse listResponse = namespace.listNamespaces(listRequest);
    assertThat(listResponse.getNamespaces()).contains(testSchema);

    // Drop schema
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Arrays.asList(TEST_CATALOG, testSchema));
    namespace.dropNamespace(dropRequest);

    // Verify schema doesn't exist
    assertThatThrownBy(() -> namespace.describeNamespace(describeRequest))
        .isInstanceOf(LanceNamespaceException.class);
  }

  @Test
  public void testTableOperations() {
    // Create schema first
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Arrays.asList(TEST_CATALOG, testSchema));
    namespace.createNamespace(nsRequest);

    String tableName =
        "test_table_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");

    // Create empty table
    CreateEmptyTableRequest createRequest = new CreateEmptyTableRequest();
    createRequest.setId(Arrays.asList(TEST_CATALOG, testSchema, tableName));
    createRequest.setLocation("/tmp/lance-integration-test/" + testSchema + "/" + tableName);

    CreateEmptyTableResponse createResponse = namespace.createEmptyTable(createRequest);
    assertThat(createResponse.getLocation()).isNotNull();

    // Describe table
    DescribeTableRequest describeRequest = new DescribeTableRequest();
    describeRequest.setId(Arrays.asList(TEST_CATALOG, testSchema, tableName));

    DescribeTableResponse describeResponse = namespace.describeTable(describeRequest);
    assertThat(describeResponse.getLocation()).contains(tableName);

    // List tables
    ListTablesRequest listRequest = new ListTablesRequest();
    listRequest.setId(Arrays.asList(TEST_CATALOG, testSchema));

    ListTablesResponse listResponse = namespace.listTables(listRequest);
    assertThat(listResponse.getTables()).contains(tableName);

    // Deregister table
    DeregisterTableRequest deregisterRequest = new DeregisterTableRequest();
    deregisterRequest.setId(Arrays.asList(TEST_CATALOG, testSchema, tableName));
    namespace.deregisterTable(deregisterRequest);

    // Verify table doesn't exist
    assertThatThrownBy(() -> namespace.describeTable(describeRequest))
        .isInstanceOf(LanceNamespaceException.class);
  }

  @Test
  public void testCascadeDropSchemaRejected() {
    // Drop schema with cascade - should be rejected
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Arrays.asList(TEST_CATALOG, testSchema));
    dropRequest.setBehavior("Cascade");

    assertThatThrownBy(() -> namespace.dropNamespace(dropRequest))
        .isInstanceOf(InvalidInputException.class)
        .hasMessageContaining("Cascade behavior is not supported");
  }
}
