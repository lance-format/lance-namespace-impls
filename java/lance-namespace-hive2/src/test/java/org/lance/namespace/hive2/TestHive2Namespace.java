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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.TableExistsRequest;

import com.google.common.collect.Maps;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHive2Namespace {

  private static BufferAllocator allocator;
  private static LocalHive2Metastore metastore;
  private static String tmpDirBase;
  private static LanceNamespace namespace;

  @BeforeAll
  public static void setup() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    metastore = new LocalHive2Metastore();
    metastore.start();

    File file =
        createTempDirectory("TestHive2Namespace", asFileAttribute(fromString("rwxrwxrwx")))
            .toFile();
    tmpDirBase = file.getAbsolutePath();

    HiveConf hiveConf = metastore.hiveConf();
    Hive2Namespace hive2Namespace = new Hive2Namespace();
    hive2Namespace.setHadoopConf(hiveConf);
    hive2Namespace.initialize(Maps.newHashMap(), allocator);
    namespace = hive2Namespace;
  }

  @AfterAll
  public static void teardown() throws Exception {
    if (allocator != null) {
      allocator.close();
    }
    if (metastore != null) {
      metastore.stop();
    }

    if (tmpDirBase != null) {
      File file = new File(tmpDirBase);
      file.delete();
    }
  }

  @AfterEach
  public void cleanup() throws Exception {
    metastore.reset();
  }

  @Test
  public void testDescribeNonExistentTable() {
    // Setup: Create database
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Lists.list("test_db"));
    nsRequest.setMode("Create");
    namespace.createNamespace(nsRequest);

    // Test: Describe non-existent table
    DescribeTableRequest request = new DescribeTableRequest();
    request.setId(Lists.list("test_db", "non_existent"));
    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.describeTable(request));
    assertTrue(error.getMessage().contains("Table does not exist"));
  }

  @Test
  public void testDescribeNamespace() {
    // Setup: Create database
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Lists.list("test_db"));
    nsRequest.setMode("Create");

    Map<String, String> properties = Maps.newHashMap();
    properties.put("database.description", "Test database description");
    properties.put("custom_param", "custom_value");
    nsRequest.setProperties(properties);

    namespace.createNamespace(nsRequest);

    // Test: Describe existing namespace
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(Lists.list("test_db"));

    DescribeNamespaceResponse response = namespace.describeNamespace(request);

    assertEquals("Test database description", response.getProperties().get("database.description"));
    assertEquals("custom_value", response.getProperties().get("custom_param"));
    assertTrue(response.getProperties().containsKey("database.location-uri"));
    assertTrue(response.getProperties().containsKey("owner"));
    assertTrue(response.getProperties().containsKey("owner_type"));
  }

  @Test
  public void testDescribeNonExistentNamespace() {
    // Test: Describe non-existent namespace
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(Lists.list("non_existent_db"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.describeNamespace(request));
    assertTrue(error.getMessage().contains("Namespace does not exist"));
  }

  @Test
  public void testNamespaceExists() {
    // Setup: Create database
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Lists.list("test_db"));
    nsRequest.setMode("Create");
    namespace.createNamespace(nsRequest);

    // Test: Check existing namespace
    NamespaceExistsRequest request = new NamespaceExistsRequest();
    request.setId(Lists.list("test_db"));

    // Should not throw exception for existing namespace
    namespace.namespaceExists(request);
  }

  @Test
  public void testNamespaceExistsNonExistent() {
    // Test: Check non-existent namespace
    NamespaceExistsRequest request = new NamespaceExistsRequest();
    request.setId(Lists.list("non_existent_db"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.namespaceExists(request));
    assertTrue(error.getMessage().contains("Namespace does not exist"));
  }

  @Test
  public void testTableExistsNonExistent() {
    // Setup: Create database
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Lists.list("test_db"));
    nsRequest.setMode("Create");
    namespace.createNamespace(nsRequest);

    // Test: Check non-existent table
    TableExistsRequest request = new TableExistsRequest();
    request.setId(Lists.list("test_db", "non_existent_table"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.tableExists(request));
    assertTrue(error.getMessage().contains("Table does not exist"));
  }

  @Test
  public void testListTablesEmpty() {
    // Setup: Create empty database
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Lists.list("empty_db"));
    nsRequest.setMode("Create");
    namespace.createNamespace(nsRequest);

    // Test: List tables in empty database
    ListTablesRequest request = new ListTablesRequest();
    request.setId(Lists.list("empty_db"));

    ListTablesResponse response = namespace.listTables(request);

    assertEquals(0, response.getTables().size());
  }

  @Test
  public void testListTablesNonExistentDatabase() {
    // Test: List tables in non-existent database
    ListTablesRequest request = new ListTablesRequest();
    request.setId(Lists.list("non_existent_db"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.listTables(request));
    assertTrue(error.getMessage().contains("Database non_existent_db doesn't exist"));
  }

  @Test
  public void testDropNamespaceBasic() {
    // Setup: Create database
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Lists.list("test_db_basic"));
    nsRequest.setMode("Create");

    Map<String, String> properties = Maps.newHashMap();
    properties.put("database.description", "Test database for dropping");
    properties.put("custom_param", "custom_value");
    nsRequest.setProperties(properties);

    namespace.createNamespace(nsRequest);

    // Test: Drop the namespace with default behavior (RESTRICT) and mode (FAIL)
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Lists.list("test_db_basic"));

    DropNamespaceResponse response = namespace.dropNamespace(dropRequest);

    // Verify properties were returned
    assertEquals(
        "Test database for dropping", response.getProperties().get("database.description"));
    assertEquals("custom_value", response.getProperties().get("custom_param"));

    // Verify namespace was dropped
    NamespaceExistsRequest existsRequest = new NamespaceExistsRequest();
    existsRequest.setId(Lists.list("test_db_basic"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.namespaceExists(existsRequest));
    assertTrue(error.getMessage().contains("Namespace does not exist"));
  }

  @Test
  public void testDropNamespaceSkipMode() {
    // Test: Drop non-existent namespace with SKIP mode
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Lists.list("non_existent_db"));
    dropRequest.setMode("Skip");

    DropNamespaceResponse response = namespace.dropNamespace(dropRequest);

    // Should return empty properties for SKIP mode
    assertEquals(0, response.getProperties().size());
  }

  @Test
  public void testDropNamespaceFailMode() {
    // Test: Drop non-existent namespace with FAIL mode (default)
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Lists.list("non_existent_db"));
    dropRequest.setMode("Fail");

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.dropNamespace(dropRequest));
    assertTrue(error.getMessage().contains("Database non_existent_db doesn't exist"));
  }
}
