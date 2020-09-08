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

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Custom message type resolver.
 */
public class MessageTypeResolver implements SerializableTypeResolver {

  @SuppressWarnings("unchecked")
  private static final Map<Class<?>, Class<? extends TypeSerializer<?>>> DEFAULT_SERIALIZERS
      = new LinkedHashMap() {
        {
          put(Serializable.class, JavaSerializableSerializer.class);
          put(MessagingSerializable.class, MessagingSerializableSerializer.class);
        }
      };

  @Override
  public void resolve(SerializerRegistry registry) {
    for (Map.Entry<Class<?>, Class<? extends TypeSerializer<?>>> entry
        : DEFAULT_SERIALIZERS.entrySet()) {
      registry.registerDefault(entry.getKey(), entry.getValue());
    }
  }
}
