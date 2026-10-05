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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.ErrorCode;
import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.NamespaceExistsResponse;
import org.lance.namespace.model.TableExistsRequest;
import org.lance.namespace.model.TableExistsResponse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.InternalServiceException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.lance.namespace.glue.GlueNamespace.LANCE_TABLE_TYPE_VALUE;
import static org.lance.namespace.glue.GlueNamespace.TABLE_TYPE_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGlueExistsContract {

  private GlueClient glue;
  private LanceNamespace api;

  @BeforeEach
  public void setUp() {
    glue = mock(GlueClient.class);
    GlueNamespace impl = new GlueNamespace();
    impl.initialize(new GlueNamespaceConfig(), glue, null);
    api = impl;
  }

  @Test
  public void testTableExistsPresent() {
    when(glue.getTable(any(GetTableRequest.class)))
        .thenReturn(GetTableResponse.builder().table(lanceTable()).build());

    TableExistsResponse response = api.tableExists(tableRequest());

    assertNotNull(response);
  }

  @Test
  public void testTableExistsAbsent() {
    when(glue.getTable(any(GetTableRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("missing").build());

    TableNotFoundException error =
        assertThrows(TableNotFoundException.class, () -> api.tableExists(tableRequest()));
    assertEquals(ErrorCode.TABLE_NOT_FOUND, error.getErrorCode());
  }

  @Test
  public void testTableExistsPermissionDenied() {
    when(glue.getTable(any(GetTableRequest.class)))
        .thenThrow(AccessDeniedException.builder().message("denied").build());

    assertPropagates(InternalException.class, () -> api.tableExists(tableRequest()));
  }

  @Test
  public void testTableExistsServiceFailure() {
    when(glue.getTable(any(GetTableRequest.class)))
        .thenThrow(InternalServiceException.builder().message("unavailable").build());

    assertPropagates(InternalException.class, () -> api.tableExists(tableRequest()));
  }

  @Test
  public void testNamespaceExistsPresent() {
    when(glue.getDatabase(any(GetDatabaseRequest.class)))
        .thenReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("ns").build()).build());

    NamespaceExistsResponse response = api.namespaceExists(namespaceRequest());

    assertNotNull(response);
  }

  @Test
  public void testNamespaceExistsAbsent() {
    when(glue.getDatabase(any(GetDatabaseRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("missing").build());

    NamespaceNotFoundException error =
        assertThrows(
            NamespaceNotFoundException.class, () -> api.namespaceExists(namespaceRequest()));
    assertEquals(ErrorCode.NAMESPACE_NOT_FOUND, error.getErrorCode());
  }

  @Test
  public void testNamespaceExistsPermissionDenied() {
    when(glue.getDatabase(any(GetDatabaseRequest.class)))
        .thenThrow(AccessDeniedException.builder().message("denied").build());

    assertPropagates(InternalException.class, () -> api.namespaceExists(namespaceRequest()));
  }

  @Test
  public void testNamespaceExistsServiceFailure() {
    when(glue.getDatabase(any(GetDatabaseRequest.class)))
        .thenThrow(InternalServiceException.builder().message("unavailable").build());

    assertPropagates(InternalException.class, () -> api.namespaceExists(namespaceRequest()));
  }

  private static Table lanceTable() {
    return Table.builder()
        .databaseName("ns")
        .name("tbl")
        .storageDescriptor(StorageDescriptor.builder().location("s3://bucket/tbl").build())
        .parameters(ImmutableMap.of(TABLE_TYPE_PROP, LANCE_TABLE_TYPE_VALUE))
        .build();
  }

  private static TableExistsRequest tableRequest() {
    return new TableExistsRequest().id(ImmutableList.of("ns", "tbl"));
  }

  private static NamespaceExistsRequest namespaceRequest() {
    return new NamespaceExistsRequest().id(ImmutableList.of("ns"));
  }

  private static void assertPropagates(
      Class<? extends LanceNamespaceException> type,
      org.junit.jupiter.api.function.Executable call) {
    LanceNamespaceException error = assertThrows(type, call);
    assertEquals(ErrorCode.INTERNAL, error.getErrorCode());
    assertNotEquals(ErrorCode.TABLE_NOT_FOUND, error.getErrorCode());
    assertNotEquals(ErrorCode.NAMESPACE_NOT_FOUND, error.getErrorCode());
  }
}
