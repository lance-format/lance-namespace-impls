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

public class TestHive3Namespace {

  private static BufferAllocator allocator;
  private static LocalHive3Metastore metastore;
  private static String tmpDirBase;
  private static LanceNamespace namespace;

  @BeforeAll
  public static void setup() throws IOException {
    allocator = new RootAllocator(Long.MAX_VALUE);
    metastore = new LocalHive3Metastore();
    metastore.start();

    File file =
        createTempDirectory("TestHive3Namespace", asFileAttribute(fromString("rwxrwxrwx")))
            .toFile();
    tmpDirBase = file.getAbsolutePath();

    HiveConf hiveConf = metastore.hiveConf();
    Hive3Namespace hive3Namespace = new Hive3Namespace();
    hive3Namespace.setHadoopConf(hiveConf);
    hive3Namespace.initialize(Maps.newHashMap(), allocator);
    namespace = hive3Namespace;

    // Setup: Create catalog and database for tests
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("catalog.location.uri", "file://" + tmpDirBase + "/test_catalog");
    nsRequest.setProperties(properties);
    nsRequest.setId(Lists.list("test_catalog"));
    nsRequest.setMode("Create");
    namespace.createNamespace(nsRequest);

    nsRequest.setId(Lists.list("test_catalog", "test_db"));
    namespace.createNamespace(nsRequest);
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

    // Re-setup catalog and database after cleanup
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("catalog.location.uri", "file://" + tmpDirBase + "/test_catalog");
    nsRequest.setProperties(properties);
    nsRequest.setId(Lists.list("test_catalog"));
    nsRequest.setMode("Create");
    namespace.createNamespace(nsRequest);

    nsRequest.setId(Lists.list("test_catalog", "test_db"));
    namespace.createNamespace(nsRequest);
  }

  @Test
  public void testDescribeNonExistentTable() {
    // Test: Describe non-existent table
    DescribeTableRequest request = new DescribeTableRequest();
    request.setId(Lists.list("test_catalog", "test_db", "non_existent"));
    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.describeTable(request));
    assertTrue(error.getMessage().contains("Table does not exist"));
  }

  @Test
  public void testDescribeNamespaceCatalog() {
    // Test: Describe catalog-level namespace
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(Lists.list("test_catalog"));

    DescribeNamespaceResponse response = namespace.describeNamespace(request);

    assertEquals("Lance catalog: test_catalog", response.getProperties().get("description"));
    assertTrue(response.getProperties().containsKey("catalog.location.uri"));
  }

  @Test
  public void testDescribeNamespaceDatabase() {
    // Test: Describe database-level namespace
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(Lists.list("test_catalog", "test_db"));

    DescribeNamespaceResponse response = namespace.describeNamespace(request);

    assertTrue(response.getProperties().containsKey("database.location-uri"));
    assertTrue(response.getProperties().containsKey("database.owner"));
    assertTrue(response.getProperties().containsKey("database.owner-type"));
  }

  @Test
  public void testDescribeNamespaceDatabaseWithCustomProperties() {
    // Setup: Create database with custom properties
    CreateNamespaceRequest nsRequest = new CreateNamespaceRequest();
    nsRequest.setId(Lists.list("test_catalog", "custom_db"));
    nsRequest.setMode("Create");

    Map<String, String> properties = Maps.newHashMap();
    properties.put("database.description", "Custom database description");
    properties.put("custom_param", "custom_value");
    nsRequest.setProperties(properties);

    namespace.createNamespace(nsRequest);

    // Test: Describe namespace with custom properties
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(Lists.list("test_catalog", "custom_db"));

    DescribeNamespaceResponse response = namespace.describeNamespace(request);

    assertEquals(
        "Custom database description", response.getProperties().get("database.description"));
    assertEquals("custom_value", response.getProperties().get("custom_param"));
    assertTrue(response.getProperties().containsKey("database.location-uri"));
    assertTrue(response.getProperties().containsKey("database.owner"));
    assertTrue(response.getProperties().containsKey("database.owner-type"));
  }

  @Test
  public void testDescribeNonExistentCatalog() {
    // Test: Describe non-existent catalog
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(Lists.list("non_existent_catalog"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.describeNamespace(request));
    assertTrue(error.getMessage().contains("Namespace does not exist"));
  }

  @Test
  public void testDescribeNonExistentDatabase() {
    // Test: Describe non-existent database
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(Lists.list("test_catalog", "non_existent_db"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.describeNamespace(request));
    assertTrue(error.getMessage().contains("Namespace does not exist"));
  }

  @Test
  public void testNamespaceExistsCatalog() {
    // Test: Check existing catalog
    NamespaceExistsRequest request = new NamespaceExistsRequest();
    request.setId(Lists.list("test_catalog"));

    // Should not throw exception for existing catalog
    namespace.namespaceExists(request);
  }

  @Test
  public void testNamespaceExistsDatabase() {
    // Test: Check existing database
    NamespaceExistsRequest request = new NamespaceExistsRequest();
    request.setId(Lists.list("test_catalog", "test_db"));

    // Should not throw exception for existing database
    namespace.namespaceExists(request);
  }

  @Test
  public void testNamespaceExistsNonExistentCatalog() {
    // Test: Check non-existent catalog
    NamespaceExistsRequest request = new NamespaceExistsRequest();
    request.setId(Lists.list("non_existent_catalog"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.namespaceExists(request));
    assertTrue(error.getMessage().contains("Namespace does not exist"));
  }

  @Test
  public void testNamespaceExistsNonExistentDatabase() {
    // Test: Check non-existent database
    NamespaceExistsRequest request = new NamespaceExistsRequest();
    request.setId(Lists.list("test_catalog", "non_existent_db"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.namespaceExists(request));
    assertTrue(error.getMessage().contains("Namespace does not exist"));
  }

  @Test
  public void testTableExistsNonExistent() {
    // Test: Check non-existent table
    TableExistsRequest request = new TableExistsRequest();
    request.setId(Lists.list("test_catalog", "test_db", "non_existent_table"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.tableExists(request));
    assertTrue(error.getMessage().contains("Table does not exist"));
  }

  @Test
  public void testListTablesEmpty() {
    // Test: List tables in empty database
    ListTablesRequest request = new ListTablesRequest();
    request.setId(Lists.list("test_catalog", "test_db"));

    ListTablesResponse response = namespace.listTables(request);

    assertEquals(0, response.getTables().size());
  }

  @Test
  public void testListTablesNonExistentDatabase() {
    // Test: List tables in non-existent database
    ListTablesRequest request = new ListTablesRequest();
    request.setId(Lists.list("test_catalog", "non_existent_db"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.listTables(request));
    assertTrue(error.getMessage().contains("Database test_catalog.non_existent_db doesn't exist"));
  }

  @Test
  public void testListTablesNonExistentCatalog() {
    // Test: List tables in non-existent catalog
    ListTablesRequest request = new ListTablesRequest();
    request.setId(Lists.list("non_existent_catalog", "test_db"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.listTables(request));
    assertTrue(error.getMessage().contains("Catalog non_existent_catalog doesn't exist"));
  }

  @Test
  public void testDropNamespaceBasicDatabase() throws IOException {
    // Setup: Create catalog and database
    CreateNamespaceRequest catalogRequest = new CreateNamespaceRequest();
    catalogRequest.setId(Lists.list("test_catalog_basic_db"));
    catalogRequest.setMode("Create");
    namespace.createNamespace(catalogRequest);

    CreateNamespaceRequest dbRequest = new CreateNamespaceRequest();
    dbRequest.setId(Lists.list("test_catalog_basic_db", "test_db"));
    dbRequest.setMode("Create");

    Map<String, String> properties = Maps.newHashMap();
    properties.put("database.description", "Test database for dropping");
    properties.put("custom_param", "custom_value");
    dbRequest.setProperties(properties);

    namespace.createNamespace(dbRequest);

    // Test: Drop the database with default behavior (RESTRICT) and mode (FAIL)
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Lists.list("test_catalog_basic_db", "test_db"));

    DropNamespaceResponse response = namespace.dropNamespace(dropRequest);

    // Verify properties were returned
    assertEquals(
        "Test database for dropping", response.getProperties().get("database.description"));
    assertEquals("custom_value", response.getProperties().get("custom_param"));

    // Verify database was dropped
    NamespaceExistsRequest existsRequest = new NamespaceExistsRequest();
    existsRequest.setId(Lists.list("test_catalog_basic_db", "test_db"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.namespaceExists(existsRequest));
    assertTrue(error.getMessage().contains("Namespace does not exist"));
  }

  @Test
  public void testDropNamespaceBasicCatalog() {
    // Setup: Create catalog
    CreateNamespaceRequest catalogRequest = new CreateNamespaceRequest();
    catalogRequest.setId(Lists.list("test_catalog_basic"));
    catalogRequest.setMode("Create");

    Map<String, String> properties = Maps.newHashMap();
    properties.put("description", "Test catalog for dropping");
    catalogRequest.setProperties(properties);

    namespace.createNamespace(catalogRequest);

    // Test: Drop the catalog with CASCADE (since Hive creates default database automatically)
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Lists.list("test_catalog_basic"));
    dropRequest.setBehavior("Cascade");

    DropNamespaceResponse response = namespace.dropNamespace(dropRequest);

    // Verify properties were returned
    assertEquals("Test catalog for dropping", response.getProperties().get("description"));

    // Verify catalog was dropped
    NamespaceExistsRequest existsRequest = new NamespaceExistsRequest();
    existsRequest.setId(Lists.list("test_catalog_basic"));

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.namespaceExists(existsRequest));
    assertTrue(error.getMessage().contains("Namespace does not exist"));
  }

  @Test
  public void testDropNamespaceSkipMode() {
    // Test: Drop non-existent database with SKIP mode
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Lists.list("non_existent_catalog", "non_existent_db"));
    dropRequest.setMode("Skip");

    DropNamespaceResponse response = namespace.dropNamespace(dropRequest);

    // Should return empty properties for SKIP mode
    assertEquals(0, response.getProperties().size());
  }

  @Test
  public void testDropNamespaceFailMode() {
    // Test: Drop non-existent database with FAIL mode (default)
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Lists.list("non_existent_catalog", "non_existent_db"));
    dropRequest.setMode("Fail");

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.dropNamespace(dropRequest));
    assertTrue(error.getMessage().contains("doesn't exist"));
  }

  @Test
  public void testDropCatalogRestrictWithDatabases() {
    // Setup: Create catalog and database
    CreateNamespaceRequest catalogRequest = new CreateNamespaceRequest();
    catalogRequest.setId(Lists.list("test_catalog_restrict_db"));
    catalogRequest.setMode("Create");
    namespace.createNamespace(catalogRequest);

    CreateNamespaceRequest dbRequest = new CreateNamespaceRequest();
    dbRequest.setId(Lists.list("test_catalog_restrict_db", "test_db"));
    dbRequest.setMode("Create");
    namespace.createNamespace(dbRequest);

    // Test: Try to drop catalog with RESTRICT behavior (should fail)
    DropNamespaceRequest dropRequest = new DropNamespaceRequest();
    dropRequest.setId(Lists.list("test_catalog_restrict_db"));
    dropRequest.setBehavior("Restrict");

    Exception error =
        assertThrows(LanceNamespaceException.class, () -> namespace.dropNamespace(dropRequest));
    assertTrue(error.getMessage().contains("is not empty"));
    assertTrue(error.getMessage().contains("databases"));
  }
}
