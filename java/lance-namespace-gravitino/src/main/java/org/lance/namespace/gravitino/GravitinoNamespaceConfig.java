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
package org.lance.namespace.gravitino;

import java.util.Map;

/** Configuration for Gravitino REST namespace. */
public class GravitinoNamespaceConfig {

  public static final String ENDPOINT = "endpoint";
  public static final String AUTH_TOKEN = "auth_token";
  public static final String CONNECT_TIMEOUT = "connect_timeout";
  public static final String READ_TIMEOUT = "read_timeout";
  public static final String MAX_RETRIES = "max_retries";

  private final String endpoint;
  private final String authToken;
  private final int connectTimeout;
  private final int readTimeout;
  private final int maxRetries;

  public GravitinoNamespaceConfig(Map<String, String> properties) {
    this.endpoint = properties.get(ENDPOINT);
    if (this.endpoint == null || this.endpoint.isEmpty()) {
      throw new IllegalArgumentException("Required property 'endpoint' is not set");
    }

    this.authToken = properties.get(AUTH_TOKEN);
    this.connectTimeout = Integer.parseInt(properties.getOrDefault(CONNECT_TIMEOUT, "10000"));
    this.readTimeout = Integer.parseInt(properties.getOrDefault(READ_TIMEOUT, "30000"));
    this.maxRetries = Integer.parseInt(properties.getOrDefault(MAX_RETRIES, "3"));
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getAuthToken() {
    return authToken;
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

  public String getBaseApiUrl() {
    String baseUrl =
        endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
    return baseUrl + "/lance/v1";
  }
}
