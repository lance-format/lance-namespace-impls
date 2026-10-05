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
import org.lance.namespace.errors.ErrorCode;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.ServiceUnavailableException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.NamespaceExistsResponse;
import org.lance.namespace.model.TableExistsRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHive3ExistsContract {

  private IMetaStoreClient client;
  private LanceNamespace api;

  @BeforeEach
  public void setUp() throws Exception {
    client = mock(IMetaStoreClient.class);
    Hive3Namespace impl = new Hive3Namespace();
    Field field = Hive3Namespace.class.getDeclaredField("clientPool");
    field.setAccessible(true);
    field.set(impl, new DirectHive3Pool(client));
    api = impl;
  }

  @Test
  public void testTableExistsPresent() throws Exception {
    when(client.getTable("spark", "db", "tbl")).thenReturn(lanceTable("s3://bucket/tbl"));

    assertNotNull(api.tableExists(tableRequest()));
  }

  @Test
  public void testLanceTableWithoutLocationStillExists() throws Exception {
    when(client.getTable("spark", "db", "tbl")).thenReturn(lanceTable(null));

    assertNotNull(api.tableExists(tableRequest()));
    TableNotFoundException described =
        assertThrows(TableNotFoundException.class, () -> api.describeTable(describeRequest()));
    assertEquals(ErrorCode.TABLE_NOT_FOUND, described.getErrorCode());
  }

  @Test
  public void testTableExistsAbsent() throws Exception {
    when(client.getTable("spark", "db", "tbl")).thenThrow(new NoSuchObjectException("missing"));

    TableNotFoundException error =
        assertThrows(TableNotFoundException.class, () -> api.tableExists(tableRequest()));
    assertEquals(ErrorCode.TABLE_NOT_FOUND, error.getErrorCode());
  }

  @Test
  public void testTableExistsPermissionDenied() throws Exception {
    when(client.getTable("spark", "db", "tbl")).thenThrow(new MetaException("Permission denied"));

    assertServiceFailure(() -> api.tableExists(tableRequest()));
  }

  @Test
  public void testTableExistsServiceFailure() throws Exception {
    when(client.getTable("spark", "db", "tbl"))
        .thenThrow(new TTransportException("metastore unavailable"));

    assertServiceFailure(() -> api.tableExists(tableRequest()));
  }

  @Test
  public void testNamespaceExistsPresent() throws Exception {
    when(client.getDatabase("spark", "db")).thenReturn(new Database());

    NamespaceExistsResponse response = api.namespaceExists(namespaceRequest());

    assertNotNull(response);
  }

  @Test
  public void testNamespaceExistsAbsent() throws Exception {
    when(client.getDatabase("spark", "db")).thenThrow(new NoSuchObjectException("missing"));

    NamespaceNotFoundException error =
        assertThrows(
            NamespaceNotFoundException.class, () -> api.namespaceExists(namespaceRequest()));
    assertEquals(ErrorCode.NAMESPACE_NOT_FOUND, error.getErrorCode());
  }

  @Test
  public void testNamespaceExistsPermissionDenied() throws Exception {
    when(client.getDatabase("spark", "db")).thenThrow(new MetaException("Permission denied"));

    assertServiceFailure(() -> api.namespaceExists(namespaceRequest()));
  }

  @Test
  public void testNamespaceExistsServiceFailure() throws Exception {
    when(client.getDatabase("spark", "db"))
        .thenThrow(new TTransportException("metastore unavailable"));

    assertServiceFailure(() -> api.namespaceExists(namespaceRequest()));
  }

  private static Table lanceTable(String location) {
    Table table = new Table();
    table.setDbName("db");
    table.setTableName("tbl");
    Map<String, String> parameters = new HashMap<>();
    parameters.put("table_type", "lance");
    table.setParameters(parameters);
    if (location != null) {
      org.apache.hadoop.hive.metastore.api.StorageDescriptor sd =
          new org.apache.hadoop.hive.metastore.api.StorageDescriptor();
      sd.setLocation(location);
      table.setSd(sd);
    }
    return table;
  }

  private static TableExistsRequest tableRequest() {
    TableExistsRequest request = new TableExistsRequest();
    request.setId(Arrays.asList("spark", "db", "tbl"));
    return request;
  }

  private static DescribeTableRequest describeRequest() {
    DescribeTableRequest request = new DescribeTableRequest();
    request.setId(tableRequest().getId());
    return request;
  }

  private static NamespaceExistsRequest namespaceRequest() {
    NamespaceExistsRequest request = new NamespaceExistsRequest();
    request.setId(Arrays.asList("spark", "db"));
    return request;
  }

  private static void assertServiceFailure(org.junit.jupiter.api.function.Executable call) {
    LanceNamespaceException error = assertThrows(ServiceUnavailableException.class, call);
    assertEquals(ErrorCode.SERVICE_UNAVAILABLE, error.getErrorCode());
    assertNotEquals(ErrorCode.TABLE_NOT_FOUND, error.getErrorCode());
    assertNotEquals(ErrorCode.NAMESPACE_NOT_FOUND, error.getErrorCode());
  }

  /** Returns the injected client without opening a metastore connection. */
  private static final class DirectHive3Pool extends Hive3ClientPool {
    private final IMetaStoreClient client;

    private DirectHive3Pool(IMetaStoreClient client) {
      super(1, new Configuration());
      this.client = client;
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action)
        throws TException, InterruptedException {
      return action.run(client);
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
        throws TException, InterruptedException {
      return action.run(client);
    }
  }
}
