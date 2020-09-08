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
 * Default serializer factory.
 * <p>
 * The default serializer factory constructs {@link TypeSerializer} instances
 * given a serializer {@link Class}. The serializer
 * must implement a default no-argument constructor.
 */
public class DefaultTypeSerializerFactory implements TypeSerializerFactory {
  @SuppressWarnings("rawtypes")
  private final Class<? extends TypeSerializer> mType;

  /**
   * Constructs a new {@link DefaultTypeSerializerFactory}.
   *
   * @param type type of serializer to create
   */
  @SuppressWarnings("rawtypes")
  public DefaultTypeSerializerFactory(Class<? extends TypeSerializer> type) {
    if (type == null) {
      throw new NullPointerException("Type cannot be null");
    }
    mType = type;
  }

  @Override
  public TypeSerializer<?> createSerializer(Class<?> type) {
    try {
      return mType.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new MessagingException("Failed to instantiate serializer: "
          + mType, e);
    }
  }
}
