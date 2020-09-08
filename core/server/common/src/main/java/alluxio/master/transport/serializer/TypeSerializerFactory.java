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

/**
 * Serializer factory.
 * <p>
 * When serializers are {@link Serializer#register(Class)}  registered}
 * with a {@link Serializer} instance. a serializer instance may be created
 * more than once. Users should implement a custom serializer factory
 * when setup needs to be done each type a {@link TypeSerializer} is created.
 */
public interface TypeSerializerFactory {

  /**
   * Creates a new serializer.
   *
   * @param type The serializable type
   * @return The created serializer
   */
  TypeSerializer<?> createSerializer(Class<?> type);
}
