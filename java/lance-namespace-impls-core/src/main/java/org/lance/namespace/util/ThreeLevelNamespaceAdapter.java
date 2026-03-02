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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;

import org.apache.arrow.memory.BufferAllocator;

import java.util.List;
import java.util.Map;

/**
 * Adapter that enforces exactly 3 levels for table identifiers and 2 levels for namespace
 * identifiers.
 *
 * <p>This adapter wraps any {@link LanceNamespace} implementation and validates that:
 *
 * <ul>
 *   <li>Table identifiers have exactly 3 levels: [catalog, database, table]
 *   <li>Namespace identifiers have at most 2 levels: [catalog] or [catalog, database]
 *   <li>Operations on non-conforming identifiers are rejected with {@link InvalidInputException}
 * </ul>
 *
 * <p>This is useful for namespace implementations that need to enforce a strict 3-level hierarchy
 * (e.g., Unity Catalog, Gravitino) while allowing the underlying implementation to be more
 * flexible.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * LanceNamespace underlying = new SomeNamespace();
 * LanceNamespace enforced = new ThreeLevelNamespaceAdapter(underlying);
 * enforced.initialize(properties, allocator);
 * }</pre>
 */
public class ThreeLevelNamespaceAdapter implements LanceNamespace {

  private final LanceNamespace delegate;

  public ThreeLevelNamespaceAdapter(LanceNamespace delegate) {
    this.delegate = delegate;
  }

  @Override
  public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
    delegate.initialize(configProperties, allocator);
  }

  @Override
  public String namespaceId() {
    return "ThreeLevelNamespaceAdapter { delegate: " + delegate.namespaceId() + " }";
  }

  @Override
  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    validateNamespaceId(request.getId(), "list");
    return delegate.listNamespaces(request);
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    validateNamespaceId(request.getId(), "create");
    return delegate.createNamespace(request);
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
    validateNamespaceId(request.getId(), "describe");
    return delegate.describeNamespace(request);
  }

  @Override
  public DropNamespaceResponse dropNamespace(DropNamespaceRequest request) {
    validateNamespaceId(request.getId(), "drop");
    return delegate.dropNamespace(request);
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    validateNamespaceId(request.getId(), "list tables in");
    return delegate.listTables(request);
  }

  @Override
  public DeclareTableResponse declareTable(DeclareTableRequest request) {
    validateTableId(request.getId());
    return delegate.declareTable(request);
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    validateTableId(request.getId());
    return delegate.describeTable(request);
  }

  @Override
  public DeregisterTableResponse deregisterTable(DeregisterTableRequest request) {
    validateTableId(request.getId());
    return delegate.deregisterTable(request);
  }

  /**
   * Validates that a namespace identifier has at most 2 levels.
   *
   * @param id the namespace identifier to validate
   * @param operation the operation being performed (for error messages)
   * @throws InvalidInputException if the identifier has more than 2 levels
   */
  private void validateNamespaceId(List<String> id, String operation) {
    if (id == null) {
      return; // Root namespace is allowed
    }

    int levels = id.size();
    if (levels > 2) {
      throw new InvalidInputException(
          String.format(
              "Cannot %s namespace with %d levels. Expected at most 2 levels (catalog.database), but got: %s",
              operation, levels, String.join(".", id)),
          "INVALID_NAMESPACE_LEVELS",
          String.join(".", id));
    }
  }

  /**
   * Validates that a table identifier has exactly 3 levels.
   *
   * @param id the table identifier to validate
   * @throws InvalidInputException if the identifier does not have exactly 3 levels
   */
  private void validateTableId(List<String> id) {
    if (id == null || id.size() != 3) {
      String idStr = id != null ? String.join(".", id) : "null";
      int levels = id != null ? id.size() : 0;
      throw new InvalidInputException(
          String.format(
              "Table identifier must have exactly 3 levels (catalog.database.table), but got %d levels: %s",
              levels, idStr),
          "INVALID_TABLE_LEVELS",
          idStr);
    }

    // Validate that no level is null or empty
    for (int i = 0; i < id.size(); i++) {
      String level = id.get(i);
      if (level == null || level.isEmpty()) {
        throw new InvalidInputException(
            String.format(
                "Table identifier level %d cannot be null or empty: %s", i, String.join(".", id)),
            "INVALID_TABLE_IDENTIFIER",
            String.join(".", id));
      }
    }
  }
}
