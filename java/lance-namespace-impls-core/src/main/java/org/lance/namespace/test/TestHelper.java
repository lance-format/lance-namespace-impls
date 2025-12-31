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
package org.lance.namespace.test;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;

/** Test utilities for creating Arrow IPC data. */
public final class TestHelper {

  private TestHelper() {}

  /**
   * Creates test Arrow IPC data with a simple schema (id: int32, name: utf8).
   *
   * @param allocator Arrow buffer allocator
   * @return Arrow IPC stream bytes
   */
  public static byte[] createTestArrowData(BufferAllocator allocator) {
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("name", ArrowType.Utf8.INSTANCE)));

    return createArrowIpcStream(allocator, schema);
  }

  /**
   * Creates empty Arrow IPC data with a simple schema.
   *
   * @param allocator Arrow buffer allocator
   * @return Arrow IPC stream bytes
   */
  public static byte[] createEmptyArrowData(BufferAllocator allocator) {
    return createTestArrowData(allocator);
  }

  /**
   * Creates Arrow IPC stream bytes from a schema.
   *
   * @param allocator Arrow buffer allocator
   * @param schema Arrow schema
   * @return Arrow IPC stream bytes
   */
  public static byte[] createArrowIpcStream(BufferAllocator allocator, Schema schema) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
        writer.start();
        writer.end();
      }
      return out.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Arrow IPC stream", e);
    }
  }
}
