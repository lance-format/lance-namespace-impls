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
package org.lance.namespace.glue;

import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableAlreadyExistsException;
import org.lance.namespace.errors.TableNotFoundException;

import software.amazon.awssdk.services.glue.model.GlueException;

public class GlueToLanceErrorConverter {

  private GlueToLanceErrorConverter() {}

  public static LanceNamespaceException notFound(GlueException e, String message, Object... args) {
    return new TableNotFoundException(
        String.format(message, args), e.getClass().getSimpleName(), e.requestId());
  }

  public static LanceNamespaceException namespaceNotFound(
      GlueException e, String message, Object... args) {
    return new NamespaceNotFoundException(
        String.format(message, args), e.getClass().getSimpleName(), e.requestId());
  }

  public static LanceNamespaceException conflict(GlueException e, String message, Object... args) {
    return new NamespaceAlreadyExistsException(
        String.format(message, args), e.getClass().getSimpleName(), e.requestId());
  }

  public static LanceNamespaceException tableConflict(
      GlueException e, String message, Object... args) {
    return new TableAlreadyExistsException(
        String.format(message, args), e.getClass().getSimpleName(), e.requestId());
  }

  public static LanceNamespaceException serverError(
      GlueException e, String message, Object... args) {
    return new InternalException(
        String.format(message, args), e.getClass().getSimpleName(), e.requestId());
  }
}
