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

import org.lance.Dataset;
import org.lance.ReadOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Utility methods for Lance table metadata shared by namespace implementations. */
public final class LanceTableUtil {

  private LanceTableUtil() {}

  public static Map<String, String> mergeTableProperties(
      Map<String, String> properties, Map<String, String> requiredProperties) {
    Map<String, String> result = new HashMap<>();
    if (properties != null) {
      result.putAll(properties);
    }
    if (requiredProperties != null) {
      result.putAll(requiredProperties);
    }
    return result;
  }

  public static boolean includeDeclared(Boolean includeDeclared) {
    return includeDeclared == null || includeDeclared;
  }

  public static boolean isOnlyDeclared(String location, Map<String, String> storageOptions) {
    return !hasStorageComponents(location, storageOptions);
  }

  public static boolean hasStorageComponents(String location, Map<String, String> storageOptions) {
    if (location == null || location.isEmpty()) {
      return false;
    }

    Map<String, String> options = storageOptions != null ? storageOptions : Collections.emptyMap();
    ReadOptions readOptions = new ReadOptions.Builder().setStorageOptions(options).build();
    try (Dataset ignored = Dataset.open(location, readOptions)) {
      return true;
    } catch (RuntimeException | LinkageError e) {
      return false;
    }
  }
}
