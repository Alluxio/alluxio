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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serializer registry.
 */
public class SerializerRegistry {
  private final Map<Class<?>, TypeSerializerFactory> mFactories = new ConcurrentHashMap<>();
  private final Map<Class<?>, TypeSerializerFactory> mDefaultFactories
      = Collections.synchronizedMap(new LinkedHashMap<>(1024, 0.75f, true));
  private final Map<Class<?>, Integer> mIds = new ConcurrentHashMap<>();
  private final Map<Integer, Class<?>> mTypes = new ConcurrentHashMap<>();

  /**
   * Constructs a new {@link SerializerRegistry}.
   *
   * @param resolvers resolvers to resolve
   */
  public SerializerRegistry(Collection<SerializableTypeResolver> resolvers) {
    resolve(new MessageTypeResolver());
    resolve(resolvers);
  }

  /**
   * Resolves serializable types with the given resolver.
   * <p>
   * This allows users to modify the serializable types registered to
   * an existing {@link Serializer} instance. Types resolved by the provided resolver(s)
   * will be added to existing types resolved by any type resolvers provided to this object's
   * constructor or by previous calls to this method.
   *
   * @param resolvers The resolvers with which to resolve serializable types
   * @return The serializer registry instance
   */
  @SuppressWarnings("unchecked")
  public SerializerRegistry resolve(SerializableTypeResolver... resolvers) {
    return resolve(resolvers != null ? Arrays.asList(resolvers) : Collections.EMPTY_LIST);
  }

  /**
   * Resolves serializable types with the given resolver.
   * <p>
   * This allows users to modify the serializable types registered
   * to an existing {@link Serializer} instance. Types resolved by the provided resolver(s)
   * will be added to existing types resolved by any type resolvers provided to this object's
   * constructor or by previous calls to this method.
   *
   * @param resolvers The resolvers with which to resolve serializable types
   * @return The serializer registry instance
   */
  public SerializerRegistry resolve(Collection<SerializableTypeResolver> resolvers) {
    if (resolvers != null) {
      for (SerializableTypeResolver resolver : resolvers) {
        resolver.resolve(this);
      }
    }
    return this;
  }

  /**
   * Returns the type ID for the given class.
   */
  private int calculateTypeId(Class<?> type) {
    if (type == null) {
      throw new NullPointerException("type cannot be null");
    }
    return hash32(type.getName());
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The type class
   * @return The serializer registry
   * @throws MessagingException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type) {
    if (type == null) {
      throw new NullPointerException("type cannot be null");
    }
    return register(type, calculateTypeId(type));
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class
   * @param id The serialization ID
   * @return The serializer registry
   * @throws MessagingException If the given {@code type} is already registered or if no default
   *         serializer could be found for the given type.
   */
  public synchronized SerializerRegistry register(Class<?> type, int id) {
    if (type == null) {
      throw new NullPointerException("type cannot be null");
    }

    // Search for a default serializer for the type.
    Class<?> baseType = findBaseType(type, mDefaultFactories);
    if (baseType == null) {
      throw new MessagingException("no default serializer found for type: " + type);
    }
    return register(type, id, mDefaultFactories.get(baseType));
  }

  /**
   * Registers a serializer for the given class.
   *
   * @param type The serializable class
   * @param serializer The serializer
   * @return The serializer registry
   * @throws MessagingException If the given {@code type} is already registered
   */
  @SuppressWarnings("rawtypes")
  public SerializerRegistry register(Class<?> type, Class<? extends TypeSerializer> serializer) {
    return register(type, calculateTypeId(type), new DefaultTypeSerializerFactory(serializer));
  }

  /**
   * Registers a serializer for the given class.
   *
   * @param type The serializable class
   * @param factory The serializer factory
   * @return The serializer registry
   * @throws MessagingException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type, TypeSerializerFactory factory) {
    return register(type, calculateTypeId(type), factory);
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class
   * @param id The serializable type ID
   * @param serializer The serializer
   * @return The serializer registry
   * @throws MessagingException If the given {@code type} is already registered
   */
  @SuppressWarnings("rawtypes")
  public SerializerRegistry register(Class<?> type, int id,
      Class<? extends TypeSerializer> serializer) {
    return register(type, id, new DefaultTypeSerializerFactory(serializer));
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class
   * @param factory The serializer factory
   * @param id The serializable type ID
   * @return The serializer registry
   * @throws MessagingException If the given {@code type} or {@code id} is already registered
   */
  public synchronized SerializerRegistry register(Class<?> type, int id,
      TypeSerializerFactory factory) {
    if (type == null) {
      throw new NullPointerException("type cannot be null");
    }

    // If the type ID has already been registered, throw an exception.
    if (mTypes.containsKey(id) && mTypes.get(id) != type) {
      throw new MessagingException("serializable type ID already registered: " + id);
    }

    // If the type has already been registered, throw an exception if the IDs don't match.
    if (mIds.containsKey(type)) {
      if (mIds.get(type) != id) {
        throw new MessagingException("type registered with a different ID: " + type);
      }
      return this;
    }

    mFactories.put(type, factory);
    mTypes.put(id, type);
    mIds.put(type, id);

    return this;
  }

  /**
   * Registers the given class as a default serializer for the given base type.
   *
   * @param baseType The base type for which to register the serializer
   * @param serializer The serializer class
   * @return The serializer registry
   */
  public SerializerRegistry registerDefault(Class<?> baseType,
      Class<? extends TypeSerializer> serializer) {
    return registerDefault(baseType, new DefaultTypeSerializerFactory(serializer));
  }

  /**
   * Registers the given factory as a default serializer factory for the given base type.
   *
   * @param baseType The base type for which to register the serializer
   * @param factory The serializer factory
   * @return The serializer registry
   */
  public synchronized SerializerRegistry registerDefault(Class<?> baseType,
      TypeSerializerFactory factory) {
    mDefaultFactories.put(baseType, factory);
    return this;
  }

  /**
   * Finds a serializable base type for the given type in the given factories map.
   */
  private Class<?> findBaseType(Class<?> type, Map<Class<?>, TypeSerializerFactory> factories) {
    if (factories.containsKey(type)) {
      return type;
    }
    List<Map.Entry<Class<?>, TypeSerializerFactory>> orderedFactories
        = new ArrayList<>(factories.entrySet());
    Collections.reverse(orderedFactories);

    Optional<Map.Entry<Class<?>, TypeSerializerFactory>> optional
        = orderedFactories.stream()
        .filter(e -> e.getKey().isAssignableFrom(type)).findFirst();
    return optional.isPresent() ? optional.get().getKey() : null;
  }

  /**
   * Looks up the serializer for the given class else {@code null} if no serializer
   * is registered for the {@code type}.
   *
   * @param type The serializable class
   * @return The serializer for the given class
   */
  synchronized TypeSerializerFactory factory(Class<?> type) {
    TypeSerializerFactory factory = mFactories.get(type);
    if (factory != null) {
      return factory;
    }

    Class<?> baseType;

    // If no factory was found, determine if a default serializer can be used.
    baseType = findBaseType(type, mDefaultFactories);
    if (baseType != null) {
      return mDefaultFactories.get(baseType);
    }
    return null;
  }

  /**
   * Looks up the serializable type ID for the given type.
   */
  synchronized int id(Class<?> type) {
    Integer id = mIds.get(type);
    if (id != null) {
      return id;
    }
    return 0;
  }

  /**
   * Returns the type for the given ID.
   *
   * @param id The ID for which to return the type
   * @return The type for the given ID
   */
  Class<?> type(int id) {
    return mTypes.get(id);
  }

  private int hash32(String value) {
    byte[] bytes = value.getBytes();
    int h = 0;
    int length = bytes.length;

    for (int i = 0; i < length; ++i) {
      h = 31 * h + bytes[i];
    }

    return h;
  }
}

