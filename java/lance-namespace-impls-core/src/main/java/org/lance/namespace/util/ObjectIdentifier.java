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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a hierarchical object identifier (namespace or table).
 *
 * <p>An identifier consists of one or more levels, where each level is a string. For example:
 *
 * <ul>
 *   <li>A root identifier has 0 levels
 *   <li>A catalog identifier has 1 level (e.g., ["my_catalog"])
 *   <li>A database identifier has 2 levels (e.g., ["my_catalog", "my_database"])
 *   <li>A table identifier has 3 levels (e.g., ["my_catalog", "my_database", "my_table"])
 * </ul>
 */
public class ObjectIdentifier {
  private static final ObjectIdentifier ROOT = new ObjectIdentifier(Collections.emptyList());
  private final List<String> levels;

  private ObjectIdentifier(List<String> levels) {
    this.levels = Collections.unmodifiableList(new ArrayList<>(levels));
  }

  public static ObjectIdentifier root() {
    return ROOT;
  }

  public static ObjectIdentifier of(List<String> levels) {
    if (levels == null || levels.isEmpty()) {
      return ROOT;
    }
    return new ObjectIdentifier(levels);
  }

  public static ObjectIdentifier of(Set<String> levels) {
    if (levels == null || levels.isEmpty()) {
      return ROOT;
    }
    return new ObjectIdentifier(new ArrayList<>(levels));
  }

  public static ObjectIdentifier of(String... levels) {
    if (levels == null || levels.length == 0) {
      return ROOT;
    }
    return new ObjectIdentifier(Arrays.asList(levels));
  }

  public boolean isRoot() {
    return levels.isEmpty();
  }

  public int levels() {
    return levels.size();
  }

  public String levelAtListPos(int index) {
    if (index < 0 || index >= levels.size()) {
      throw new IndexOutOfBoundsException(
          "Index " + index + " out of bounds for identifier with " + levels.size() + " levels");
    }
    return levels.get(index);
  }

  public List<String> getLevels() {
    return levels;
  }

  public ObjectIdentifier parent() {
    if (isRoot()) {
      throw new IllegalStateException("Root identifier has no parent");
    }
    return of(levels.subList(0, levels.size() - 1));
  }

  public String name() {
    if (isRoot()) {
      throw new IllegalStateException("Root identifier has no name");
    }
    return levels.get(levels.size() - 1);
  }

  public ObjectIdentifier child(String name) {
    List<String> newLevels = new ArrayList<>(levels);
    newLevels.add(name);
    return of(newLevels);
  }

  @Override
  public String toString() {
    if (isRoot()) {
      return "[]";
    }
    return levels.toString();
  }

  public String toDelimited(String delimiter) {
    return String.join(delimiter, levels);
  }

  public List<String> listStyleId() {
    return levels;
  }

  public String stringStyleId() {
    return String.join(".", levels);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    ObjectIdentifier that = (ObjectIdentifier) obj;
    return Objects.equals(levels, that.levels);
  }

  @Override
  public int hashCode() {
    return Objects.hash(levels);
  }
}
