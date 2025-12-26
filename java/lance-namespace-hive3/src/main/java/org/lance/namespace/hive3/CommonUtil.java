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

/** Common utility methods. */
public class CommonUtil {

  private CommonUtil() {}

  public static String formatCurrentStackTrace() {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    StringBuilder sb = new StringBuilder();
    for (int i = 2; i < Math.min(stack.length, 10); i++) {
      sb.append(stack[i].toString()).append("\n");
    }
    return sb.toString();
  }

  public static String makeQualified(String path) {
    if (path == null) {
      return null;
    }
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }
}
