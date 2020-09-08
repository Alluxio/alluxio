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
 * The serializer type resolver is responsible for locating serializable types
 * and their serializers.
 * <p>
 * Users can implement custom type resolvers to automatically register serializers.
 * See {@link MessageTypeResolver} for an example implementation.
 */
public interface SerializableTypeResolver {

  /**
   * Registers serializable types on the given {@link SerializerRegistry} instance.
   *
   * @param registry The serializer registry
   */
  void resolve(SerializerRegistry registry);
}
