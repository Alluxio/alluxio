/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.transport.serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Provides custom object serialization.
 * <p>
 * This interface can be implemented to provide custom serialization
 * for objects of a given type.
 *
 * @param <T> type of the object to read/write
 */
public interface TypeSerializer<T> {

  /**
   * Writes the object to the given buffer.
   *
   * @param object The object to write
   * @param os The output to which to write the object
   */
  void write(T object, DataOutputStream os) throws IOException;

  /**
   * Reads the object from the given buffer.
   *
   * @param type The type to read
   * @param is The input from which to read the object
   * @return The read object
   */
  T read(Class<T> type, DataInputStream is) throws IOException, ClassNotFoundException;
}
