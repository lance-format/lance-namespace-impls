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

import java.util.Collections;
import java.util.List;

/** Represents a hierarchical identifier for namespaces and tables. */
public class ObjectIdentifier {
  private final List<String> levels;

  private ObjectIdentifier(List<String> levels) {
    this.levels = levels != null ? levels : Collections.emptyList();
  }

  public static ObjectIdentifier of(List<String> levels) {
    return new ObjectIdentifier(levels);
  }

  public boolean isRoot() {
    return levels.isEmpty();
  }

  public int levels() {
    return levels.size();
  }

  public String levelAtListPos(int pos) {
    if (pos < 0 || pos >= levels.size()) {
      throw new IndexOutOfBoundsException(
          "Position " + pos + " is out of bounds for size " + levels.size());
    }
    return levels.get(pos);
  }

  public String stringStyleId() {
    return String.join(".", levels);
  }

  @Override
  public String toString() {
    return stringStyleId();
  }
}
