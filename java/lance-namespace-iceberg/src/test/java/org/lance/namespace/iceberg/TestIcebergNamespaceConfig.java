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
package org.lance.namespace.iceberg;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestIcebergNamespaceConfig {

  @Test
  public void testConfigWithEndpoint() {
    Map<String, String> properties = new HashMap<>();
    properties.put("endpoint", "http://localhost:8181");

    IcebergNamespaceConfig config = new IcebergNamespaceConfig(properties);

    assertEquals("http://localhost:8181", config.getEndpoint());
    assertEquals(System.getProperty("user.dir"), config.getRoot());
    assertNull(config.getAuthToken());
    assertNull(config.getCredential());
    assertEquals(10000, config.getConnectTimeout());
    assertEquals(30000, config.getReadTimeout());
    assertEquals(3, config.getMaxRetries());
  }

  @Test
  public void testConfigWithAllProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("endpoint", "https://iceberg.example.com");
    properties.put("auth_token", "test_token");
    properties.put("credential", "client_id:client_secret");
    properties.put("connect_timeout", "5000");
    properties.put("read_timeout", "60000");
    properties.put("max_retries", "5");
    properties.put("root", "/data/lance");

    IcebergNamespaceConfig config = new IcebergNamespaceConfig(properties);

    assertEquals("https://iceberg.example.com", config.getEndpoint());
    assertEquals("test_token", config.getAuthToken());
    assertEquals("client_id:client_secret", config.getCredential());
    assertEquals(5000, config.getConnectTimeout());
    assertEquals(60000, config.getReadTimeout());
    assertEquals(5, config.getMaxRetries());
    assertEquals("/data/lance", config.getRoot());
  }

  @Test
  public void testMissingEndpointThrowsException() {
    Map<String, String> properties = new HashMap<>();

    assertThrows(IllegalArgumentException.class, () -> new IcebergNamespaceConfig(properties));
  }

  @Test
  public void testEmptyEndpointThrowsException() {
    Map<String, String> properties = new HashMap<>();
    properties.put("endpoint", "");

    assertThrows(IllegalArgumentException.class, () -> new IcebergNamespaceConfig(properties));
  }

  @Test
  public void testGetBaseApiUrlStripsTrailingSlash() {
    Map<String, String> properties = new HashMap<>();
    properties.put("endpoint", "http://localhost:8181/");

    IcebergNamespaceConfig config = new IcebergNamespaceConfig(properties);

    assertEquals("http://localhost:8181", config.getBaseApiUrl());
  }

  @Test
  public void testGetBaseApiUrlWithoutTrailingSlash() {
    Map<String, String> properties = new HashMap<>();
    properties.put("endpoint", "http://localhost:8181");

    IcebergNamespaceConfig config = new IcebergNamespaceConfig(properties);

    assertEquals("http://localhost:8181", config.getBaseApiUrl());
  }
}
