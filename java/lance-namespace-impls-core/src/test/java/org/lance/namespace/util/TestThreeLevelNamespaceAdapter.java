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
package org.lance.namespace.util;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.InvalidInputException;
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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestThreeLevelNamespaceAdapter {

  private BufferAllocator allocator;
  private LanceNamespace mockDelegate;
  private ThreeLevelNamespaceAdapter adapter;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator();
    mockDelegate = mock(LanceNamespace.class);
    adapter = new ThreeLevelNamespaceAdapter(mockDelegate);
  }

  @AfterEach
  void tearDown() {
    allocator.close();
  }

  @Test
  void testInitialize() {
    Map<String, String> config = Collections.singletonMap("key", "value");
    adapter.initialize(config, allocator);
    verify(mockDelegate).initialize(config, allocator);
  }

  @Test
  void testNamespaceId() {
    when(mockDelegate.namespaceId()).thenReturn("MockNamespace");
    String id = adapter.namespaceId();
    assertTrue(id.contains("ThreeLevelNamespaceAdapter"));
    assertTrue(id.contains("MockNamespace"));
  }

  // Namespace operations tests

  @Test
  void testListNamespacesWithRootId() {
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setId(null);

    ListNamespacesResponse response = new ListNamespacesResponse();
    when(mockDelegate.listNamespaces(request)).thenReturn(response);

    assertEquals(response, adapter.listNamespaces(request));
    verify(mockDelegate).listNamespaces(request);
  }

  @Test
  void testListNamespacesWithOneLevelId() {
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setId(Collections.singletonList("catalog"));

    ListNamespacesResponse response = new ListNamespacesResponse();
    when(mockDelegate.listNamespaces(request)).thenReturn(response);

    assertEquals(response, adapter.listNamespaces(request));
    verify(mockDelegate).listNamespaces(request);
  }

  @Test
  void testListNamespacesWithTwoLevelId() {
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setId(Arrays.asList("catalog", "database"));

    ListNamespacesResponse response = new ListNamespacesResponse();
    when(mockDelegate.listNamespaces(request)).thenReturn(response);

    assertEquals(response, adapter.listNamespaces(request));
    verify(mockDelegate).listNamespaces(request);
  }

  @Test
  void testListNamespacesWithThreeLevelIdFails() {
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setId(Arrays.asList("catalog", "database", "table"));

    InvalidInputException exception =
        assertThrows(InvalidInputException.class, () -> adapter.listNamespaces(request));

    assertTrue(exception.getMessage().contains("3 levels"));
    assertTrue(exception.getMessage().contains("at most 2 levels"));
    verify(mockDelegate, never()).listNamespaces(any());
  }

  @Test
  void testCreateNamespaceWithValidId() {
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setId(Arrays.asList("catalog", "database"));

    CreateNamespaceResponse response = new CreateNamespaceResponse();
    when(mockDelegate.createNamespace(request)).thenReturn(response);

    assertEquals(response, adapter.createNamespace(request));
    verify(mockDelegate).createNamespace(request);
  }

  @Test
  void testCreateNamespaceWithInvalidIdFails() {
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setId(Arrays.asList("catalog", "database", "extra"));

    InvalidInputException exception =
        assertThrows(InvalidInputException.class, () -> adapter.createNamespace(request));

    assertTrue(exception.getMessage().contains("3 levels"));
    verify(mockDelegate, never()).createNamespace(any());
  }

  @Test
  void testDescribeNamespace() {
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(Collections.singletonList("catalog"));

    DescribeNamespaceResponse response = new DescribeNamespaceResponse();
    when(mockDelegate.describeNamespace(request)).thenReturn(response);

    assertEquals(response, adapter.describeNamespace(request));
    verify(mockDelegate).describeNamespace(request);
  }

  @Test
  void testDropNamespace() {
    DropNamespaceRequest request = new DropNamespaceRequest();
    request.setId(Arrays.asList("catalog", "database"));

    DropNamespaceResponse response = new DropNamespaceResponse();
    when(mockDelegate.dropNamespace(request)).thenReturn(response);

    assertEquals(response, adapter.dropNamespace(request));
    verify(mockDelegate).dropNamespace(request);
  }

  // Table operations tests

  @Test
  void testDeclareTableWithValidId() {
    DeclareTableRequest request = new DeclareTableRequest();
    request.setId(Arrays.asList("catalog", "database", "table"));

    DeclareTableResponse response = new DeclareTableResponse();
    when(mockDelegate.declareTable(request)).thenReturn(response);

    assertEquals(response, adapter.declareTable(request));
    verify(mockDelegate).declareTable(request);
  }

  @Test
  void testDeclareTableWithTwoLevelIdFails() {
    DeclareTableRequest request = new DeclareTableRequest();
    request.setId(Arrays.asList("catalog", "database"));

    InvalidInputException exception =
        assertThrows(InvalidInputException.class, () -> adapter.declareTable(request));

    assertTrue(exception.getMessage().contains("exactly 3 levels"));
    assertTrue(exception.getMessage().contains("2 levels"));
    verify(mockDelegate, never()).declareTable(any());
  }

  @Test
  void testDeclareTableWithFourLevelIdFails() {
    DeclareTableRequest request = new DeclareTableRequest();
    request.setId(Arrays.asList("catalog", "database", "table", "extra"));

    InvalidInputException exception =
        assertThrows(InvalidInputException.class, () -> adapter.declareTable(request));

    assertTrue(exception.getMessage().contains("exactly 3 levels"));
    assertTrue(exception.getMessage().contains("4 levels"));
    verify(mockDelegate, never()).declareTable(any());
  }

  @Test
  void testDeclareTableWithNullIdFails() {
    DeclareTableRequest request = new DeclareTableRequest();
    request.setId(null);

    InvalidInputException exception =
        assertThrows(InvalidInputException.class, () -> adapter.declareTable(request));

    assertTrue(exception.getMessage().contains("exactly 3 levels"));
    verify(mockDelegate, never()).declareTable(any());
  }

  @Test
  void testDeclareTableWithEmptyLevelFails() {
    DeclareTableRequest request = new DeclareTableRequest();
    request.setId(Arrays.asList("catalog", "", "table"));

    InvalidInputException exception =
        assertThrows(InvalidInputException.class, () -> adapter.declareTable(request));

    assertTrue(exception.getMessage().contains("cannot be null or empty"));
    verify(mockDelegate, never()).declareTable(any());
  }

  @Test
  void testDescribeTable() {
    DescribeTableRequest request = new DescribeTableRequest();
    request.setId(Arrays.asList("catalog", "database", "table"));

    DescribeTableResponse response = new DescribeTableResponse();
    when(mockDelegate.describeTable(request)).thenReturn(response);

    assertEquals(response, adapter.describeTable(request));
    verify(mockDelegate).describeTable(request);
  }

  @Test
  void testDeregisterTable() {
    DeregisterTableRequest request = new DeregisterTableRequest();
    request.setId(Arrays.asList("catalog", "database", "table"));

    DeregisterTableResponse response = new DeregisterTableResponse();
    when(mockDelegate.deregisterTable(request)).thenReturn(response);

    assertEquals(response, adapter.deregisterTable(request));
    verify(mockDelegate).deregisterTable(request);
  }

  @Test
  void testListTables() {
    ListTablesRequest request = new ListTablesRequest();
    request.setId(Arrays.asList("catalog", "database"));

    ListTablesResponse response = new ListTablesResponse();
    when(mockDelegate.listTables(request)).thenReturn(response);

    assertEquals(response, adapter.listTables(request));
    verify(mockDelegate).listTables(request);
  }

  @Test
  void testListTablesWithInvalidIdFails() {
    ListTablesRequest request = new ListTablesRequest();
    request.setId(Arrays.asList("catalog", "database", "table"));

    InvalidInputException exception =
        assertThrows(InvalidInputException.class, () -> adapter.listTables(request));

    assertTrue(exception.getMessage().contains("3 levels"));
    verify(mockDelegate, never()).listTables(any());
  }
}
