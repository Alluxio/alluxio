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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a special {@link TypeSerializer} implementation
 * that handles serialization for {@link MessagingSerializable} objects.
 *
 * @param <T> type of the serializable
 */
public class MessagingSerializableSerializer<T extends MessagingSerializable>
    implements TypeSerializer<T> {
  private final Map<Class<?>, Constructor<?>> mConstructorMap = new HashMap<>();

  @Override
  public void write(T object, DataOutputStream os) throws IOException {
    object.writeObject(os);
  }

  @Override
  public T read(Class<T> type, DataInputStream is) throws IOException, ClassNotFoundException {
    return readObject(type, is);
  }

  /**
   * Reads an object.
   *
   * @param type The object type
   * @param is The object input stream
   * @return The object
   */
  @SuppressWarnings("unchecked")
  private T readObject(Class<T> type, DataInputStream is)
      throws IOException, ClassNotFoundException {
    try {
      Constructor<?> constructor = mConstructorMap.get(type);
      if (constructor == null) {
        try {
          constructor = type.getDeclaredConstructor();
          constructor.setAccessible(true);
          mConstructorMap.put(type, constructor);
        } catch (NoSuchMethodException e) {
          throw new MessagingException(
              "Failed to instantiate reference: must provide a single argument constructor", e);
        }
      }

      T object = (T) constructor.newInstance();
      object.readObject(is);
      return object;
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new MessagingException(
          "Failed to instantiate object: must provide a no argument constructor", e);
    }
  }
}
