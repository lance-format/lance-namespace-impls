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

import org.lance.namespace.errors.InvalidInputException;
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
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
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
 * Integration tests for Hive2Namespace against a running Hive 2.x Metastore.
 *
 * <p>To run these tests, start Hive 2.x Metastore with: cd docker && make up-hive2
 *
 * <p>Default endpoint: thrift://localhost:9083
 *
 * <p>Tests are automatically skipped if Hive Metastore is not available.
 */
public class TestHive2NamespaceIntegration {

  private static final String METASTORE_HOST = "localhost";
  private static final int METASTORE_PORT = 9083;
  private static final String METASTORE_URI = "thrift://" + METASTORE_HOST + ":" + METASTORE_PORT;
  private static boolean hiveAvailable = false;

  private Hive2Namespace namespace;
  private BufferAllocator allocator;
  private String testDatabase;

  @BeforeAll
  public static void checkHiveAvailable() {
    try (Socket socket = new Socket(METASTORE_HOST, METASTORE_PORT)) {
      hiveAvailable = true;
      System.out.println("Hive 2.x Metastore detected at " + METASTORE_URI);
    } catch (IOException e) {
      hiveAvailable = false;
      System.out.println(
          "Hive 2.x Metastore is not available at "
              + METASTORE_URI
              + " ("
              + e.getMessage()
              + ") - skipping integration tests");
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    Assumptions.assumeTrue(hiveAvailable, "Hive 2.x Metastore is not available");

    allocator = new RootAllocator();
    namespace = new Hive2Namespace();

    String uniqueId = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    testDatabase = "test_db_" + uniqueId;

    // Set up Hadoop configuration with metastore URI
    Configuration hadoopConf = new Configuration();
    hadoopConf.set(HiveConf.ConfVars.METASTOREURIS.varname, METASTORE_URI);
    namespace.setConf(hadoopConf);

    Map<String, String> config = new HashMap<>();
    config.put("client.pool-size", "3");
    config.put("root", "/tmp/lance-integration-test");

    namespace.initialize(config, allocator);
  }

  @AfterEach
  public void tearDown() {
    try {
      // Clean up test database
      DropNamespaceRequest dropRequest = new DropNamespaceRequest();
      dropRequest.setId(Collections.singletonList(testDatabase));
      dropRequest.setBehavior("Restrict");
      namespace.dropNamespace(dropRequest);
    } catch (Exception e) {
      // Ignore cleanup errors
    }

    // Namespace cleanup handled by Hive internals

    if (allocator != null) {
      allocator.close();
    }
  }

  @Test
  public void testListDatabases() {
    // Hive 2.x uses 2-level namespace: database.table
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setId(Collections.emptyList());

    ListNamespacesResponse response = namespace.listNamespaces(request);
    assertThat(response.getNamespaces()).isNotEmpty();
    // Default database should always exist
    assertThat(response.getNamespaces()).contains("default");
  }

  @Test
  public void testDatabaseOperations() {
    // Create database
    CreateNamespaceRequest createRequest = new CreateNamespaceRequest();
    createRequest.setId(Collections.singletonList(testDatabase));
    Map<String, String> props = new HashMap<>();
    props.put("database.description", "Integration test database");
    createRequest.setProperties(props);

    CreateNamespaceResponse createResponse = namespace.createNamespace(createRequest);
    assertThat(createResponse).isNotNull();

    // Describe database
    DescribeNamespaceRequest describeRequest = new DescribeNamespaceRequest();
    describeRequest.setId(Collections.singletonList(testDatabase));

    DescribeNamespaceResponse describeResponse = namespace.describeNamespace(describeRequest);
    assertThat(describeResponse).isNotNull();
    assertThat(describeResponse.getProperties())
        .containsEntry("database.description", "Integration test database");

    // List databases
    ListNamespacesRequest listRequest = new ListNamespacesRequest();
    listRequest.setId(Collections.emptyList());

    ListNamespacesResponse listResponse = namespace.listNamespaces(listRequest);
    assertThat(listResponse.getNamespaces()).contains(testDatabase);

    // Drop database
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Collections.singletonList(testDatabase));
    namespace.dropNamespace(dropRequest);

    // Verify database doesn't exist
    assertThatThrownBy(() -> namespace.describeNamespace(describeRequest))
        .isInstanceOf(LanceNamespaceException.class);
  }

  @Test
  public void testTableOperations() {
    // Create database first
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Collections.singletonList(testDatabase));
    namespace.createNamespace(nsRequest);

    String tableName =
        "test_table_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");

    // Declare table
    DeclareTableRequest createRequest = new DeclareTableRequest();
    createRequest.setId(Arrays.asList(testDatabase, tableName));
    createRequest.setLocation("/tmp/lance-integration-test/" + testDatabase + "/" + tableName);

    DeclareTableResponse createResponse = namespace.declareTable(createRequest);
    assertThat(createResponse.getLocation()).isNotNull();

    // Describe table
    DescribeTableRequest describeRequest = new DescribeTableRequest();
    describeRequest.setId(Arrays.asList(testDatabase, tableName));

    DescribeTableResponse describeResponse = namespace.describeTable(describeRequest);
    assertThat(describeResponse.getLocation()).contains(tableName);

    // List tables
    ListTablesRequest listRequest = new ListTablesRequest();
    listRequest.setId(Collections.singletonList(testDatabase));

    ListTablesResponse listResponse = namespace.listTables(listRequest);
    assertThat(listResponse.getTables()).contains(tableName);

    // Deregister table
    DeregisterTableRequest deregisterRequest = new DeregisterTableRequest();
    deregisterRequest.setId(Arrays.asList(testDatabase, tableName));
    namespace.deregisterTable(deregisterRequest);

    // Verify table doesn't exist
    assertThatThrownBy(() -> namespace.describeTable(describeRequest))
        .isInstanceOf(LanceNamespaceException.class);

    // Declare table again for dropping.
    createResponse = namespace.declareTable(createRequest);
    assertThat(createResponse.getLocation()).isNotNull();

    // Drop table
    DropTableRequest dropTableRequest = new DropTableRequest();
    dropTableRequest.setId(Arrays.asList(testDatabase, tableName));
    namespace.dropTable(dropTableRequest);

    // Verify table doesn't exist
    assertThatThrownBy(() -> namespace.describeTable(describeRequest))
        .isInstanceOf(LanceNamespaceException.class);
  }

  @Test
  public void testCascadeDropDatabaseRejected() {
    // Drop database with cascade - should be rejected
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Collections.singletonList(testDatabase));
    dropRequest.setBehavior("Cascade");

    assertThatThrownBy(() -> namespace.dropNamespace(dropRequest))
        .isInstanceOf(InvalidInputException.class)
        .hasMessageContaining("Cascade behavior is not supported");
  }
}
