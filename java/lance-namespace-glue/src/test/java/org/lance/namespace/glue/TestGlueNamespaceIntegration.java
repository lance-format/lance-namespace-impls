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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for GlueNamespace against a real AWS Glue catalog.
 *
 * <p>To run these tests locally:
 *
 * <ol>
 *   <li>Configure AWS credentials (via environment variables, ~/.aws/credentials, or IAM role)
 *   <li>Set AWS_S3_BUCKET_NAME environment variable
 *   <li>Run: make integ-test-glue
 * </ol>
 *
 * <p>Tests are automatically skipped if AWS credentials are not available.
 */
public class TestGlueNamespaceIntegration {

  private static final String AWS_REGION =
      System.getenv("AWS_REGION") != null ? System.getenv("AWS_REGION") : "us-east-1";
  private static final String AWS_S3_BUCKET_NAME = System.getenv("AWS_S3_BUCKET_NAME");
  private static boolean awsCredentialsAvailable = false;
  private static String s3Root;

  private GlueNamespace namespace;
  private BufferAllocator allocator;
  private String testDatabase;
  private List<String> createdDatabases;

  @BeforeAll
  public static void checkAwsCredentialsAvailable() {
    // Check if S3 bucket is configured
    if (AWS_S3_BUCKET_NAME == null || AWS_S3_BUCKET_NAME.isEmpty()) {
      System.out.println("AWS_S3_BUCKET_NAME not set - skipping integration tests");
      awsCredentialsAvailable = false;
      return;
    }

    // Check if AWS credentials are available via environment variables
    String accessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
    String secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");

    if (accessKeyId != null
        && !accessKeyId.isEmpty()
        && secretAccessKey != null
        && !secretAccessKey.isEmpty()) {
      awsCredentialsAvailable = true;
      System.out.println("AWS credentials found in environment variables");
    } else {
      // Try to use default credentials chain by making a simple API call
      try {
        software.amazon.awssdk.services.sts.StsClient stsClient =
            software.amazon.awssdk.services.sts.StsClient.builder()
                .region(software.amazon.awssdk.regions.Region.of(AWS_REGION))
                .build();
        stsClient.getCallerIdentity();
        stsClient.close();
        awsCredentialsAvailable = true;
        System.out.println("AWS credentials found via default credentials chain");
      } catch (Exception e) {
        awsCredentialsAvailable = false;
        System.out.println(
            "AWS credentials not available (" + e.getMessage() + ") - skipping integration tests");
      }
    }

    if (awsCredentialsAvailable) {
      String uniqueId = UUID.randomUUID().toString().substring(0, 8);
      s3Root = "s3://" + AWS_S3_BUCKET_NAME + "/lance_glue_test_" + uniqueId;
      System.out.println("Using S3 root: " + s3Root);
    }
  }

  @BeforeEach
  public void setUp() {
    Assumptions.assumeTrue(awsCredentialsAvailable, "AWS credentials are not available");

    allocator = new RootAllocator();
    namespace = new GlueNamespace();
    createdDatabases = new ArrayList<>();

    String uniqueId = UUID.randomUUID().toString().substring(0, 8);
    testDatabase = "lance_test_db_" + uniqueId;

    Map<String, String> config = new HashMap<>();
    config.put("region", AWS_REGION);
    config.put("root", s3Root);

    namespace.initialize(config, allocator);
  }

  @AfterEach
  public void tearDown() {
    // Clean up test resources
    for (String dbName : createdDatabases) {
      try {
        cleanupDatabase(dbName);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
    }

    if (namespace != null) {
      namespace.close();
    }

    if (allocator != null) {
      allocator.close();
    }
  }

  private void cleanupDatabase(String databaseName) {
    try {
      // First, delete all tables in the database
      ListTablesRequest listRequest = new ListTablesRequest();
      listRequest.setId(Collections.singletonList(databaseName));
      ListTablesResponse listResponse = namespace.listTables(listRequest);

      for (String tableName : listResponse.getTables()) {
        try {
          DeregisterTableRequest deregRequest = new DeregisterTableRequest();
          deregRequest.setId(Arrays.asList(databaseName, tableName));
          namespace.deregisterTable(deregRequest);
        } catch (Exception e) {
          // Ignore
        }
      }

      // Then drop the database
      DropNamespaceRequest dropRequest = new DropNamespaceRequest();
      dropRequest.setId(Collections.singletonList(databaseName));
      namespace.dropNamespace(dropRequest);
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  private String createTestDatabase(String suffix) {
    String dbName = "lance_test_" + UUID.randomUUID().toString().substring(0, 8) + suffix;
    createdDatabases.add(dbName);

    CreateNamespaceRequest createRequest = new CreateNamespaceRequest();
    createRequest.setId(Collections.singletonList(dbName));
    createRequest.setProperties(
        Collections.singletonMap("description", "Lance integration test database"));
    namespace.createNamespace(createRequest);

    return dbName;
  }

  @Test
  public void testNamespaceOperations() {
    String dbName = "lance_test_" + UUID.randomUUID().toString().substring(0, 8);
    createdDatabases.add(dbName);

    // Create namespace
    CreateNamespaceRequest createRequest = new CreateNamespaceRequest();
    createRequest.setId(Collections.singletonList(dbName));
    createRequest.setProperties(Collections.singletonMap("description", "Test database for Lance"));

    CreateNamespaceResponse createResponse = namespace.createNamespace(createRequest);
    assertThat(createResponse).isNotNull();

    // Describe namespace
    DescribeNamespaceRequest describeRequest = new DescribeNamespaceRequest();
    describeRequest.setId(Collections.singletonList(dbName));

    DescribeNamespaceResponse describeResponse = namespace.describeNamespace(describeRequest);
    assertThat(describeResponse).isNotNull();
    assertThat(describeResponse.getProperties())
        .containsEntry("description", "Test database for Lance");

    // Check namespace exists
    NamespaceExistsRequest existsRequest = new NamespaceExistsRequest();
    existsRequest.setId(Collections.singletonList(dbName));
    namespace.namespaceExists(existsRequest); // Should not throw

    // List namespaces
    ListNamespacesRequest listRequest = new ListNamespacesRequest();
    listRequest.setId(Collections.emptyList());
    ListNamespacesResponse listResponse = namespace.listNamespaces(listRequest);
    assertThat(listResponse.getNamespaces()).contains(dbName);

    // Drop namespace
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Collections.singletonList(dbName));
    namespace.dropNamespace(dropRequest);
    createdDatabases.remove(dbName);

    // Verify namespace doesn't exist
    assertThatThrownBy(() -> namespace.namespaceExists(existsRequest))
        .isInstanceOf(LanceNamespaceException.class);
  }

  @Test
  public void testTableOperations() {
    String dbName = createTestDatabase("");
    String tableName = "test_table_" + UUID.randomUUID().toString().substring(0, 8);
    String tableLocation = s3Root + "/" + dbName + "/" + tableName + ".lance";

    // Declare table
    DeclareTableRequest createRequest = new DeclareTableRequest();
    createRequest.setId(Arrays.asList(dbName, tableName));
    createRequest.setLocation(tableLocation);

    DeclareTableResponse createResponse = namespace.declareTable(createRequest);
    assertThat(createResponse.getLocation()).isNotNull();
    assertThat(createResponse.getLocation()).isEqualTo(tableLocation);

    // Describe table
    DescribeTableRequest describeRequest = new DescribeTableRequest();
    describeRequest.setId(Arrays.asList(dbName, tableName));

    DescribeTableResponse describeResponse = namespace.describeTable(describeRequest);
    assertThat(describeResponse.getLocation()).isNotNull();
    assertThat(describeResponse.getLocation()).isEqualTo(tableLocation);

    // Check table exists
    TableExistsRequest existsRequest = new TableExistsRequest();
    existsRequest.setId(Arrays.asList(dbName, tableName));
    namespace.tableExists(existsRequest); // Should not throw

    // List tables
    ListTablesRequest listRequest = new ListTablesRequest();
    listRequest.setId(Collections.singletonList(dbName));

    ListTablesResponse listResponse = namespace.listTables(listRequest);
    assertThat(listResponse.getTables()).contains(tableName);

    // Deregister table
    DeregisterTableRequest deregisterRequest = new DeregisterTableRequest();
    deregisterRequest.setId(Arrays.asList(dbName, tableName));
    namespace.deregisterTable(deregisterRequest);

    // Verify table doesn't exist
    assertThatThrownBy(() -> namespace.tableExists(existsRequest))
        .isInstanceOf(LanceNamespaceException.class);
  }

  @Test
  public void testMultipleTablesInNamespace() {
    String dbName = createTestDatabase("");
    List<String> tableNames = new ArrayList<>();

    // Create multiple tables
    for (int i = 0; i < 3; i++) {
      String tableName = "table_" + i + "_" + UUID.randomUUID().toString().substring(0, 6);
      tableNames.add(tableName);

      String tableLocation = s3Root + "/" + dbName + "/" + tableName + ".lance";
      DeclareTableRequest createRequest = new DeclareTableRequest();
      createRequest.setId(Arrays.asList(dbName, tableName));
      createRequest.setLocation(tableLocation);
      namespace.declareTable(createRequest);
    }

    // List tables and verify all are present
    ListTablesRequest listRequest = new ListTablesRequest();
    listRequest.setId(Collections.singletonList(dbName));

    ListTablesResponse listResponse = namespace.listTables(listRequest);
    for (String tableName : tableNames) {
      assertThat(listResponse.getTables()).contains(tableName);
    }

    // Clean up tables
    for (String tableName : tableNames) {
      DeregisterTableRequest deregisterRequest = new DeregisterTableRequest();
      deregisterRequest.setId(Arrays.asList(dbName, tableName));
      namespace.deregisterTable(deregisterRequest);
    }
  }
}
