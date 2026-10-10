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

import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.rest.RestClient;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for Unity namespace request mapping. */
public class TestUnityNamespaceUnit {
  private BufferAllocator allocator;

  @Before
  public void setUp() {
    allocator = new RootAllocator();
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testDeclareTableUsesStructFieldTypeJson() throws Exception {
    RestClient restClient = mock(RestClient.class);
    UnityModels.TableInfo tableInfo = new UnityModels.TableInfo();
    tableInfo.setProperties(Collections.singletonMap("table_type", "lance"));
    when(restClient.post(eq("/tables"), any(), eq(UnityModels.TableInfo.class)))
        .thenReturn(tableInfo);

    UnityNamespace namespace = new UnityNamespace();
    setField(namespace, "config", new UnityNamespaceConfig(requiredConfig()));
    setField(namespace, "allocator", allocator);
    setField(namespace, "restClient", restClient);

    DeclareTableRequest request = new DeclareTableRequest();
    request.setId(Arrays.asList("unity", "default", "test_table"));
    request.setLocation("/tmp/test_table");

    namespace.declareTable(request);

    ArgumentCaptor<Object> bodyCaptor = ArgumentCaptor.forClass(Object.class);
    verify(restClient).post(eq("/tables"), bodyCaptor.capture(), eq(UnityModels.TableInfo.class));

    UnityModels.CreateTable createTable = (UnityModels.CreateTable) bodyCaptor.getValue();
    UnityModels.ColumnInfo placeholderColumn = createTable.getColumns().get(0);
    assertEquals("__placeholder_id", placeholderColumn.getName());
    assertEquals(
        "{\"name\":\"__placeholder_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}",
        placeholderColumn.getTypeJson());
  }

  private Map<String, String> requiredConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("endpoint", "http://localhost:8080");
    config.put("catalog", "unity");
    return config;
  }

  private void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
