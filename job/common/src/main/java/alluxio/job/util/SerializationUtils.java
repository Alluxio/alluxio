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

package alluxio.job.util;

import com.google.common.collect.Maps;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Serialization related utility methods.
 */
@ThreadSafe
public final class SerializationUtils {
  private SerializationUtils() {} // prevent instantiation

  /**
   * Serializes an object into a byte array. When the object is null, returns null.
   *
   * @param obj the object to serialize
   * @return the serialized bytes
   * @throws IOException if the serialization fails
   */
  public static byte[] serialize(Object obj) throws IOException {
    if (obj == null) {
      return null;
    }
    try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
      try (ObjectOutputStream o = new ObjectOutputStream(b)) {
        o.writeObject(obj);
      }
      return b.toByteArray();
    }
  }

  /**
   * Wrapper around {@link #serialize(Object)} which throws a runtime exception with the given
   * message on failure.
   *
   * @param obj the object the serialize
   * @param errorMessage the message to show if serialization fails
   * @return the serialized bytes
   */
  public static byte[] serialize(Object obj, String errorMessage) {
    try {
      return serialize(obj);
    } catch (IOException e) {
      throw new RuntimeException(errorMessage, e);
    }
  }

  /**
   * Deserializes a byte array into an object. When the bytes are null, returns null.
   *
   * @param bytes the byte array to deserialize
   * @return the deserialized object
   * @throws IOException if the deserialization fails
   * @throws ClassNotFoundException if no class found to deserialize into
   */
  public static Serializable deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    if (bytes == null) {
      return null;
    }
    try (ByteArrayInputStream b = new ByteArrayInputStream(bytes)) {
      try (ObjectInputStream o = new ObjectInputStream(b)) {
        return (Serializable) o.readObject();
      }
    }
  }

  /**
   * Wrapper around {@link #deserialize(Object)} which throws a runtime exception with the given
   * message on failure.
   *
   * @param bytes the byte array the deserialize
   * @param errorMessage the message to show if deserialization fails
   * @return the deserialized object
   */
  public static Serializable deserialize(byte[] bytes, String errorMessage) {
    try {
      return deserialize(bytes);
    } catch (Exception e) {
      throw new RuntimeException(errorMessage, e);
    }
  }

  /**
   * @param <S> the key type for the Map
   * @param <T> the type of the values in the collections which are the values for the Map
   * @param map a map to make serializable
   * @return a copy of the map with serializable values
   */
  public static <S, T extends Serializable> Map<S, ArrayList<T>> makeValuesSerializable(
      Map<S, Collection<T>> map) {
    return Maps.transformValues(map, ArrayList::new);
  }
}
