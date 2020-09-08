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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Java serializable serializer implementation.
 *
 * @param <T> type of object to serializer
 */
public class JavaSerializableSerializer<T> implements TypeSerializer<T> {

  @Override
  public void write(T object, DataOutputStream buffer) throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream();
         ObjectOutputStream out = new ObjectOutputStream(os)) {
      out.writeObject(object);
      out.flush();
      byte[] bytes = os.toByteArray();
      buffer.writeInt(bytes.length);
      buffer.write(bytes);
    }
  }

  /**
   * Reads a object from input stream.
   *
   * @param type the type to read
   * @param input the data input stream
   * @return object of type T
   */
  @Override
  @SuppressWarnings("unchecked")
  public T read(Class<T> type, DataInputStream input)
      throws IOException, ClassNotFoundException {
    byte[] bytes = SerializerUtils.readBytesFromStream(input, input.readInt());
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      return (T) in.readObject();
    }
  }
}
