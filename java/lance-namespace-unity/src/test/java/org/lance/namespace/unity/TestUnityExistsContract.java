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
import org.lance.namespace.rest.RestClient;
import org.lance.namespace.rest.RestClientException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUnityExistsContract {

  private static final String NAMESPACE_PATH = "/schemas/main.db";
  private static final String TABLE_PATH = "/tables/main.db.tbl";

  private RestClient rest;
  private LanceNamespace api;

  @BeforeEach
  public void setUp() throws Exception {
    rest = mock(RestClient.class);
    Map<String, String> config = new HashMap<>();
    config.put("endpoint", "http://localhost:1");
    config.put("catalog", "main");
    UnityNamespace impl = new UnityNamespace();
    impl.initialize(config, null);
    replaceRestClient(impl, rest);
    api = impl;
  }

  @Test
  public void testTableExistsPresent() throws Exception {
    when(rest.get(eq(TABLE_PATH), eq(UnityModels.TableInfo.class))).thenReturn(lanceTable());

    TableExistsResponse response = api.tableExists(tableRequest());

    assertNotNull(response);
  }

  @Test
  public void testTableExistsAbsent() throws Exception {
    when(rest.get(eq(TABLE_PATH), eq(UnityModels.TableInfo.class)))
        .thenThrow(new RestClientException(404, "missing"));

    TableNotFoundException error =
        assertThrows(TableNotFoundException.class, () -> api.tableExists(tableRequest()));
    assertEquals(ErrorCode.TABLE_NOT_FOUND, error.getErrorCode());
  }

  @Test
  public void testTableExistsPermissionDenied() throws Exception {
    when(rest.get(eq(TABLE_PATH), eq(UnityModels.TableInfo.class)))
        .thenThrow(new RestClientException(403, "denied"));

    assertInternal(() -> api.tableExists(tableRequest()));
  }

  @Test
  public void testTableExistsServiceFailure() throws Exception {
    when(rest.get(eq(TABLE_PATH), eq(UnityModels.TableInfo.class)))
        .thenThrow(new RestClientException(503, "unavailable"));

    assertInternal(() -> api.tableExists(tableRequest()));
  }

  @Test
  public void testNamespaceExistsPresent() throws Exception {
    when(rest.get(eq(NAMESPACE_PATH), eq(UnityModels.SchemaInfo.class)))
        .thenReturn(new UnityModels.SchemaInfo());

    NamespaceExistsResponse response = api.namespaceExists(namespaceRequest());

    assertNotNull(response);
  }

  @Test
  public void testNamespaceExistsAbsent() throws Exception {
    when(rest.get(eq(NAMESPACE_PATH), eq(UnityModels.SchemaInfo.class)))
        .thenThrow(new RestClientException(404, "missing"));

    NamespaceNotFoundException error =
        assertThrows(
            NamespaceNotFoundException.class, () -> api.namespaceExists(namespaceRequest()));
    assertEquals(ErrorCode.NAMESPACE_NOT_FOUND, error.getErrorCode());
  }

  @Test
  public void testNamespaceExistsPermissionDenied() throws Exception {
    when(rest.get(eq(NAMESPACE_PATH), eq(UnityModels.SchemaInfo.class)))
        .thenThrow(new RestClientException(403, "denied"));

    assertInternal(() -> api.namespaceExists(namespaceRequest()));
  }

  @Test
  public void testNamespaceExistsServiceFailure() throws Exception {
    when(rest.get(eq(NAMESPACE_PATH), eq(UnityModels.SchemaInfo.class)))
        .thenThrow(new RestClientException(503, "unavailable"));

    assertInternal(() -> api.namespaceExists(namespaceRequest()));
  }

  private static UnityModels.TableInfo lanceTable() {
    UnityModels.TableInfo table = new UnityModels.TableInfo();
    table.setName("tbl");
    table.setStorageLocation("s3://bucket/tbl");
    table.setProperties(Collections.singletonMap("table_type", "lance"));
    return table;
  }

  private static TableExistsRequest tableRequest() {
    TableExistsRequest request = new TableExistsRequest();
    request.setId(Arrays.asList("main", "db", "tbl"));
    return request;
  }

  private static NamespaceExistsRequest namespaceRequest() {
    NamespaceExistsRequest request = new NamespaceExistsRequest();
    request.setId(Arrays.asList("main", "db"));
    return request;
  }

  private static void replaceRestClient(UnityNamespace impl, RestClient replacement)
      throws Exception {
    Field field = UnityNamespace.class.getDeclaredField("restClient");
    field.setAccessible(true);
    RestClient real = (RestClient) field.get(impl);
    field.set(impl, replacement);
    real.close();
  }

  private static void assertInternal(org.junit.jupiter.api.function.Executable call) {
    LanceNamespaceException error = assertThrows(InternalException.class, call);
    assertEquals(ErrorCode.INTERNAL, error.getErrorCode());
    assertNotEquals(ErrorCode.TABLE_NOT_FOUND, error.getErrorCode());
    assertNotEquals(ErrorCode.NAMESPACE_NOT_FOUND, error.getErrorCode());
  }
}
