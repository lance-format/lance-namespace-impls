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
package org.lance.namespace.hive2;

import java.util.Map;

public class Hive2NamespaceConfig {
  // Hive 2.x specific meta properties
  public static final String DATABASE_DESCRIPTION = "database.description";
  public static final String DATABASE_LOCATION_URI = "database.location-uri";
  public static final String DATABASE_OWNER = "database.owner";
  public static final String DATABASE_OWNER_TYPE = "database.owner-type";

  // Namespace config key
  public static final String CLIENT_POOL_SIZE = "client.pool-size";
  public static final int CLIENT_POOL_SIZE_DEFAULT = 3;

  /** Storage root location of the lakehouse on Hive catalog */
  public static final String ROOT = "root";

  public static final String ROOT_DEFAULT = System.getProperty("user.dir");

  private final int clientPoolSize;
  private final String root;

  public Hive2NamespaceConfig(Map<String, String> properties) {
    // Inline PropertyUtil.propertyAsInt
    String clientPoolSizeStr = properties.get(CLIENT_POOL_SIZE);
    this.clientPoolSize =
        clientPoolSizeStr != null ? Integer.parseInt(clientPoolSizeStr) : CLIENT_POOL_SIZE_DEFAULT;

    // Inline PropertyUtil.propertyAsString and OpenDalUtil.stripTrailingSlash
    String rootValue = properties.getOrDefault(ROOT, ROOT_DEFAULT);
    this.root =
        rootValue != null && rootValue.endsWith("/")
            ? rootValue.substring(0, rootValue.length() - 1)
            : rootValue;
  }

  public int getClientPoolSize() {
    return clientPoolSize;
  }

  public String getRoot() {
    return root;
  }
}
