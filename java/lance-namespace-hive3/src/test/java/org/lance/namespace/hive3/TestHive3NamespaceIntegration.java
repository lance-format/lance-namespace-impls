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

import org.lance.namespace.LanceNamespaceException;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.CreateEmptyTableResponse;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
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

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for Hive3Namespace against a running Hive 3.x/4.x Metastore.
 *
 * <p>To run these tests, start Hive 3.x Metastore with: cd docker && make up-hive3
 *
 * <p>Default endpoint: thrift://localhost:9084
 *
 * <p>Tests are automatically skipped if Hive Metastore is not available.
 */
public class TestHive3NamespaceIntegration {

  private static final String METASTORE_HOST = "localhost";
  private static final int METASTORE_PORT = 9084;
  private static final String METASTORE_URI = "thrift://" + METASTORE_HOST + ":" + METASTORE_PORT;
  private static boolean hiveAvailable = false;

  private Hive3Namespace namespace;
  private BufferAllocator allocator;
  private String testCatalog;
  private String testDatabase;

  @BeforeAll
  public static void checkHiveAvailable() {
    try (Socket socket = new Socket(METASTORE_HOST, METASTORE_PORT)) {
      hiveAvailable = true;
      System.out.println("Hive 3.x Metastore detected at " + METASTORE_URI);
    } catch (IOException e) {
      hiveAvailable = false;
      System.out.println(
          "Hive 3.x Metastore is not available at "
              + METASTORE_URI
              + " ("
              + e.getMessage()
              + ") - skipping integration tests");
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    Assumptions.assumeTrue(hiveAvailable, "Hive 3.x Metastore is not available");

    allocator = new RootAllocator();
    namespace = new Hive3Namespace();

    String uniqueId = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    testCatalog = "hive"; // Default catalog in Hive 3.x
    testDatabase = "test_db_" + uniqueId;

    Map<String, String> config = new HashMap<>();
    config.put("hive.metastore.uris", METASTORE_URI);
    config.put("client.pool-size", "3");
    config.put("root", "/tmp/lance-integration-test");

    namespace.initialize(config, allocator);
  }

  @AfterEach
  public void tearDown() {
    try {
      // Clean up test database
      DropNamespaceRequest dropRequest = new DropNamespaceRequest();
      dropRequest.setId(Arrays.asList(testCatalog, testDatabase));
      dropRequest.setBehavior(DropNamespaceRequest.BehaviorEnum.CASCADE);
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
  public void testListCatalogs() {
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setId(Collections.emptyList());

    ListNamespacesResponse response = namespace.listNamespaces(request);
    assertThat(response.getNamespaces()).isNotEmpty();
    assertThat(response.getNamespaces()).contains("hive");
  }

  @Test
  public void testDatabaseOperations() {
    // Create database
    CreateNamespaceRequest createRequest = new CreateNamespaceRequest();
    createRequest.setId(Arrays.asList(testCatalog, testDatabase));
    Map<String, String> props = new HashMap<>();
    props.put("database.description", "Integration test database");
    createRequest.setProperties(props);

    CreateNamespaceResponse createResponse = namespace.createNamespace(createRequest);
    assertThat(createResponse).isNotNull();

    // Describe database
    DescribeNamespaceRequest describeRequest = new DescribeNamespaceRequest();
    describeRequest.setId(Arrays.asList(testCatalog, testDatabase));

    DescribeNamespaceResponse describeResponse = namespace.describeNamespace(describeRequest);
    assertThat(describeResponse).isNotNull();
    assertThat(describeResponse.getProperties()).containsEntry(
        "database.description", "Integration test database");

    // List databases in catalog
    ListNamespacesRequest listRequest = new ListNamespacesRequest();
    listRequest.setId(Collections.singletonList(testCatalog));

    ListNamespacesResponse listResponse = namespace.listNamespaces(listRequest);
    assertThat(listResponse.getNamespaces()).contains(testDatabase);

    // Drop database
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Arrays.asList(testCatalog, testDatabase));
    namespace.dropNamespace(dropRequest);

    // Verify database doesn't exist
    assertThatThrownBy(() -> namespace.describeNamespace(describeRequest))
        .isInstanceOf(LanceNamespaceException.class);
  }

  @Test
  public void testTableOperations() {
    // Create database first
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Arrays.asList(testCatalog, testDatabase));
    namespace.createNamespace(nsRequest);

    String tableName = "test_table_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");

    // Create empty table (declare table without data)
    CreateEmptyTableRequest createRequest = new CreateEmptyTableRequest();
    createRequest.setId(Arrays.asList(testCatalog, testDatabase, tableName));
    createRequest.setLocation("/tmp/lance-integration-test/" + testDatabase + "/" + tableName);

    CreateEmptyTableResponse createResponse = namespace.createEmptyTable(createRequest);
    assertThat(createResponse.getLocation()).isNotNull();

    // Describe table
    DescribeTableRequest describeRequest = new DescribeTableRequest();
    describeRequest.setId(Arrays.asList(testCatalog, testDatabase, tableName));

    DescribeTableResponse describeResponse = namespace.describeTable(describeRequest);
    assertThat(describeResponse.getLocation()).contains(tableName);
    assertThat(describeResponse.getProperties()).containsEntry("table_type", "lance");

    // List tables
    ListTablesRequest listRequest = new ListTablesRequest();
    listRequest.setId(Arrays.asList(testCatalog, testDatabase));

    ListTablesResponse listResponse = namespace.listTables(listRequest);
    assertThat(listResponse.getTables()).contains(tableName);

    // Drop table
    DropTableRequest dropRequest = new DropTableRequest();
    dropRequest.setId(Arrays.asList(testCatalog, testDatabase, tableName));
    namespace.dropTable(dropRequest);

    // Verify table doesn't exist
    assertThatThrownBy(() -> namespace.describeTable(describeRequest))
        .isInstanceOf(LanceNamespaceException.class);
  }

  @Test
  public void testCascadeDropDatabase() {
    // Create database
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Arrays.asList(testCatalog, testDatabase));
    namespace.createNamespace(nsRequest);

    // Create a table in the database
    String tableName = "cascade_test_table";
    CreateEmptyTableRequest tableRequest = new CreateEmptyTableRequest();
    tableRequest.setId(Arrays.asList(testCatalog, testDatabase, tableName));
    tableRequest.setLocation("/tmp/lance-integration-test/" + testDatabase + "/" + tableName);
    namespace.createEmptyTable(tableRequest);

    // Drop database with cascade
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Arrays.asList(testCatalog, testDatabase));
    dropRequest.setBehavior(DropNamespaceRequest.BehaviorEnum.CASCADE);
    namespace.dropNamespace(dropRequest);

    // Verify database doesn't exist
    DescribeNamespaceRequest describeRequest = new DescribeNamespaceRequest();
    describeRequest.setId(Arrays.asList(testCatalog, testDatabase));
    assertThatThrownBy(() -> namespace.describeNamespace(describeRequest))
        .isInstanceOf(LanceNamespaceException.class);
  }
}
