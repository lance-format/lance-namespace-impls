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
package org.lance.namespace.polaris;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.ErrorCode;
import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.NamespaceExistsResponse;
import org.lance.namespace.model.TableExistsRequest;
import org.lance.namespace.rest.RestClient;
import org.lance.namespace.rest.RestClientException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPolarisExistsContract {

  private static final String NAMESPACE_PATH = "/v1/cat/namespaces/ns";
  private static final String TABLE_PATH = "/polaris/v1/cat/namespaces/ns/generic-tables/tbl";

  private RestClient rest;
  private LanceNamespace api;

  @BeforeEach
  public void setUp() throws Exception {
    rest = mock(RestClient.class);
    Map<String, String> config = new HashMap<>();
    config.put("endpoint", "http://localhost:1");
    PolarisNamespace impl = new PolarisNamespace();
    impl.initialize(config, null);
    replaceRestClient(impl, rest);
    api = impl;
  }

  @Test
  public void testTableExistsPresent() throws Exception {
    when(rest.get(eq(TABLE_PATH), eq(PolarisModels.LoadGenericTableResponse.class)))
        .thenReturn(table("lance"));

    assertNotNull(api.tableExists(tableRequest()));
  }

  @Test
  public void testNonLanceTableStillExists() throws Exception {
    when(rest.get(eq(TABLE_PATH), eq(PolarisModels.LoadGenericTableResponse.class)))
        .thenReturn(table("iceberg"));

    assertNotNull(api.tableExists(tableRequest()));
    InvalidInputException described =
        assertThrows(InvalidInputException.class, () -> api.describeTable(describeRequest()));
    assertEquals(ErrorCode.INVALID_INPUT, described.getErrorCode());
  }

  @Test
  public void testTableExistsAbsent() throws Exception {
    when(rest.get(eq(TABLE_PATH), eq(PolarisModels.LoadGenericTableResponse.class)))
        .thenThrow(new RestClientException(404, "missing"));

    TableNotFoundException error =
        assertThrows(TableNotFoundException.class, () -> api.tableExists(tableRequest()));
    assertEquals(ErrorCode.TABLE_NOT_FOUND, error.getErrorCode());
  }

  @Test
  public void testTableExistsPermissionDenied() throws Exception {
    when(rest.get(eq(TABLE_PATH), eq(PolarisModels.LoadGenericTableResponse.class)))
        .thenThrow(new RestClientException(403, "denied"));

    assertInternal(() -> api.tableExists(tableRequest()));
  }

  @Test
  public void testTableExistsServiceFailure() throws Exception {
    when(rest.get(eq(TABLE_PATH), eq(PolarisModels.LoadGenericTableResponse.class)))
        .thenThrow(new RestClientException(503, "unavailable"));

    assertInternal(() -> api.tableExists(tableRequest()));
  }

  @Test
  public void testNamespaceExistsPresent() throws Exception {
    when(rest.get(eq(NAMESPACE_PATH), eq(PolarisModels.NamespaceResponse.class)))
        .thenReturn(new PolarisModels.NamespaceResponse());

    NamespaceExistsResponse response = api.namespaceExists(namespaceRequest());

    assertNotNull(response);
  }

  @Test
  public void testNamespaceExistsAbsent() throws Exception {
    when(rest.get(eq(NAMESPACE_PATH), eq(PolarisModels.NamespaceResponse.class)))
        .thenThrow(new RestClientException(404, "missing"));

    NamespaceNotFoundException error =
        assertThrows(
            NamespaceNotFoundException.class, () -> api.namespaceExists(namespaceRequest()));
    assertEquals(ErrorCode.NAMESPACE_NOT_FOUND, error.getErrorCode());
  }

  @Test
  public void testNamespaceExistsPermissionDenied() throws Exception {
    when(rest.get(eq(NAMESPACE_PATH), eq(PolarisModels.NamespaceResponse.class)))
        .thenThrow(new RestClientException(403, "denied"));

    assertInternal(() -> api.namespaceExists(namespaceRequest()));
  }

  @Test
  public void testNamespaceExistsServiceFailure() throws Exception {
    when(rest.get(eq(NAMESPACE_PATH), eq(PolarisModels.NamespaceResponse.class)))
        .thenThrow(new RestClientException(503, "unavailable"));

    assertInternal(() -> api.namespaceExists(namespaceRequest()));
  }

  private static PolarisModels.LoadGenericTableResponse table(String format) {
    PolarisModels.GenericTable generic = new PolarisModels.GenericTable();
    generic.setName("tbl");
    generic.setFormat(format);
    generic.setBaseLocation("s3://bucket/tbl");
    PolarisModels.LoadGenericTableResponse response = new PolarisModels.LoadGenericTableResponse();
    response.setTable(generic);
    return response;
  }

  private static TableExistsRequest tableRequest() {
    TableExistsRequest request = new TableExistsRequest();
    request.setId(Arrays.asList("cat", "ns", "tbl"));
    return request;
  }

  private static DescribeTableRequest describeRequest() {
    DescribeTableRequest request = new DescribeTableRequest();
    request.setId(tableRequest().getId());
    return request;
  }

  private static NamespaceExistsRequest namespaceRequest() {
    NamespaceExistsRequest request = new NamespaceExistsRequest();
    request.setId(Arrays.asList("cat", "ns"));
    return request;
  }

  private static void replaceRestClient(PolarisNamespace impl, RestClient replacement)
      throws Exception {
    Field field = PolarisNamespace.class.getDeclaredField("restClient");
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
