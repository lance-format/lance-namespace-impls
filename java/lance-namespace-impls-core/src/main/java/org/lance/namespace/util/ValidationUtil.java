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

import org.lance.namespace.errors.InvalidInputException;

/** Utility methods for validation. */
public final class ValidationUtil {

  private ValidationUtil() {}

  public static void checkArgument(boolean condition, String message, Object... args) {
    if (!condition) {
      throw new InvalidInputException(String.format(message, args));
    }
  }

  public static void checkNotNull(Object reference, String message, Object... args) {
    if (reference == null) {
      throw new InvalidInputException(String.format(message, args));
    }
  }

  public static void checkNotEmpty(String value, String message, Object... args) {
    if (value == null || value.isEmpty()) {
      throw new InvalidInputException(String.format(message, args));
    }
  }

  public static void checkState(boolean condition, String message, Object... args) {
    if (!condition) {
      throw new IllegalStateException(String.format(message, args));
    }
  }
}
