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

import java.util.Map;

/** Configuration for Iceberg REST Catalog namespace. */
public class IcebergNamespaceConfig {

  public static final String ENDPOINT = "endpoint";
  public static final String WAREHOUSE = "warehouse";
  public static final String PREFIX = "prefix";
  public static final String AUTH_TOKEN = "auth_token";
  public static final String CREDENTIAL = "credential";
  public static final String CONNECT_TIMEOUT = "connect_timeout";
  public static final String READ_TIMEOUT = "read_timeout";
  public static final String MAX_RETRIES = "max_retries";
  public static final String ROOT = "root";

  private final String endpoint;
  private final String warehouse;
  private final String prefix;
  private final String authToken;
  private final String credential;
  private final int connectTimeout;
  private final int readTimeout;
  private final int maxRetries;
  private final String root;

  public IcebergNamespaceConfig(Map<String, String> properties) {
    this.endpoint = properties.get(ENDPOINT);
    if (this.endpoint == null || this.endpoint.isEmpty()) {
      throw new IllegalArgumentException("Required property 'endpoint' is not set");
    }

    this.warehouse = properties.get(WAREHOUSE);
    this.prefix = properties.getOrDefault(PREFIX, "");
    this.authToken = properties.get(AUTH_TOKEN);
    this.credential = properties.get(CREDENTIAL);
    this.connectTimeout = Integer.parseInt(properties.getOrDefault(CONNECT_TIMEOUT, "10000"));
    this.readTimeout = Integer.parseInt(properties.getOrDefault(READ_TIMEOUT, "30000"));
    this.maxRetries = Integer.parseInt(properties.getOrDefault(MAX_RETRIES, "3"));
    this.root = properties.getOrDefault(ROOT, System.getProperty("user.dir"));
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getWarehouse() {
    return warehouse;
  }

  public String getPrefix() {
    return prefix;
  }

  public String getAuthToken() {
    return authToken;
  }

  public String getCredential() {
    return credential;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public int getReadTimeout() {
    return readTimeout;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public String getRoot() {
    return root;
  }

  public String getFullApiUrl() {
    String base = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
    if (prefix != null && !prefix.isEmpty()) {
      return base + "/" + prefix;
    }
    return base;
  }

  public String getBaseApiUrl() {
    return endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
  }
}
