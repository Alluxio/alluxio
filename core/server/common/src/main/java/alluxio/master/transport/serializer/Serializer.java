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

import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;

import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializer to serialize java objects.
 * <p>
 * Serialization is performed by {@link TypeSerializer} instances. Objects that
 * can be serialized by {@link Serializer} must be registered. When objects
 * are serialized, Serializer will write the object's type as an 16-bit unsigned integer.
 * When reading objects, the 16-bit identifier is used to construct a new object.
 * <p>
 * Serializable objects must either provide a {@link TypeSerializer}.
 * implement {@link MessagingSerializable}, or implement
 * {@link java.io.Externalizable}.
 * <p>
 * Serialization via this class is not thread safe.
 */
public class Serializer {
  private static final Logger LOG = LoggerFactory.getLogger(Serializer.class);

  private final SerializerRegistry mRegistry;
  private final Map<Class<?>, TypeSerializer<?>> mSerializers = new HashMap<>();
  private final Map<Class<?>, Integer> mIds = new HashMap<>();
  private final Map<String, Class<?>> mTypes = new HashMap<>();

  private Supplier<RetryPolicy> mRetryPolicy;

  /**
   * Creates a new serializer instance.
   */
  @SuppressWarnings("unchecked")
  public Serializer() {
    this(Collections.EMPTY_LIST);
  }

  /**
   * Creates a new serializer instance with type resolver(s).
   * The given {@link SerializableTypeResolver}s will be used to locate serializable types on the
   * classpath. By default, the {@link MessageTypeResolver} will be used
   * to register common serializable types, and any additional types will be registered
   * via provided type resolvers thereafter.
   *
   * @param resolvers A collection of serializable type resolvers with which
   *                  to register serializable types.
   */
  public Serializer(Collection<SerializableTypeResolver> resolvers) {
    mRegistry = new SerializerRegistry(resolvers);
    mRetryPolicy = () -> RetryUtils.defaultClientRetry(
        Duration.ofMinutes(1),
        Duration.ofMillis(50),
        Duration.ofSeconds(3));
  }

  /**
   * Registers a serializable type.
   * <p>
   * The serializable type must be assignable from {@link MessagingSerializable} or
   * {@link java.io.Externalizable}.
   *
   * @param type The serializable type. This type must be assignable from
   *             {@link MessagingSerializable} or {@link java.io.Externalizable}
   * @return The serializer instance
   * @throws java.lang.IllegalArgumentException If the serializable type ID is within
   *         the reserved range `128` to `255`
   */
  public Serializer register(Class<?> type) {
    mRegistry.register(type);
    return this;
  }

  /**
   * Registers a serializable type with an identifier.
   * <p>
   * The serializable type must be assignable from {@link MessagingSerializable} or
   * {@link java.io.Externalizable}.
   *
   * @param type The serializable type. This type must be assignable from
   *             {@link MessagingSerializable} or {@link java.io.Externalizable}
   * @param id The type ID. This ID must be a number between `0` and `65535`
   *           Serialization IDs between `128` and `255` are reserved and will result in an
   *           {@link java.lang.IllegalArgumentException}
   * @return The serializer instance
   * @throws java.lang.IllegalArgumentException If the serializable type ID is within
   *         the reserved range `128` to `255`
   */
  public Serializer register(Class<?> type, int id) {
    mRegistry.register(type, id);
    return this;
  }

  /**
   * Writes an object to a buffer.
   * The given object must have a {@link Serializer#register(Class) registered} serializer
   * or implement {@link java.io.Serializable}. If a serializable type ID was provided
   * during registration, the type ID will be written to the returned
   * {@link ByteArrayOutputStream} in lieu of the class name. Types with no associated type ID
   * will be written to the buffer with a full class name for reference during serialization.
   * <p>
   * Types that implement {@link java.io.Serializable} will be serialized using Java's
   * {@link java.io.ObjectOutputStream}. Types that implement {@link java.io.Externalizable}
   * will be serialized via that interface's methods unless a custom {@link TypeSerializer}
   * has been registered for the type. {@link java.io.Externalizable} types can,
   * however, still take advantage of faster serialization of type IDs.
   *
   * @param object The object to write
   * @param <T> The object type
   * @return The serialized object
   * @throws MessagingException If no serializer is registered for the object
   */
  public <T> ByteArrayOutputStream writeObject(T object) throws MessagingException {
    RetryPolicy retryPolicy = mRetryPolicy.get();
    IOException thrownException = null;
    while (retryPolicy.attempt()) {
      try {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(byteOut);
        writeObject(object, dataOut);
        return byteOut;
      } catch (IOException e) {
        LOG.debug("Attempt {} to write object to byte array failed with exception : {}",
            retryPolicy.getAttemptCount(), e.toString());
        thrownException = e;
      }
    }
    throw new MessagingException("Failed to write object", thrownException);
  }

  /**
   * Writes an object to the given output stream.
   * Serialized bytes will be written to the given {@link DataOutputStream}.
   * The given object must have a {@link Serializer#register(Class) registered}
   * serializer or implement {@link java.io.Serializable}. If a serializable type ID
   * was provided during registration, the type ID will be written to the given
   * {@link DataOutputStream} in lieu of the class name. Types with no associated type ID
   * will be written to the buffer with a full class name for reference during serialization.
   * <p>
   * Types that implement {@link MessagingSerializable} will be serialized via
   * {@link MessagingSerializable#writeObject(DataOutputStream)} unless a
   * {@link TypeSerializer} was explicitly registered for the type.
   * <p>
   * Types that implement {@link java.io.Serializable} will be serialized using Java's
   * {@link java.io.ObjectOutputStream}. Types that implement {@link java.io.Externalizable}
   * will be serialized via that interface's methods unless a custom {@link TypeSerializer}
   * has been registered for the type. {@link java.io.Externalizable} types can,
   * however, still take advantage of faster serialization of type IDs.
   *
   * @param object The object to write
   * @param output The buffer to which to write the object
   * @param <T> The object type
   * @return The serialized object
   */
  private <T> DataOutputStream writeObject(T object, DataOutputStream output) throws IOException {
    if (object == null) {
      output.writeByte(Identifier.NULL.code());
      return output;
    }

    Class<?> type = object.getClass();

    // Look up the serializer for the given object type.
    TypeSerializer serializer = getSerializer(type);

    // If no serializer was found, throw a serialization exception.
    if (serializer == null) {
      throw new IOException("Cannot serialize unregistered type: " + type);
    }

    // Cache the serializable type ID if necessary.
    if (!mIds.containsKey(type)) {
      mIds.put(type, mRegistry.id(type));
    }

    // Lookup the serializable type ID for the type.
    int typeId = mRegistry.id(type);
    if (typeId == 0) {
      return writeByClass(type, object, output, serializer);
    }
    return writeById(typeId, object, output, serializer);
  }

  /**
   * Writes an object to the buffer using the given serialization ID.
   */
  @SuppressWarnings("unchecked")
  private DataOutputStream writeById(int id, Object object, DataOutputStream output,
      TypeSerializer serializer) throws IOException {
    for (Identifier identifier : Identifier.values()) {
      if (identifier.accept(id)) {
        output.writeByte(identifier.code());
        identifier.write(id, output);
        serializer.write(object, output);
        return output;
      }
    }
    throw new MessagingException("Invalid type ID: " + id);
  }

  /**
   * Writes an object to the buffer with its class name.
   */
  @SuppressWarnings("unchecked")
  private DataOutputStream writeByClass(Class<?> type, Object object,
      DataOutputStream output, TypeSerializer serializer) throws IOException {
    output.writeByte(Identifier.CLASS.code());
    SerializerUtils.writeStringToStream(output, type.getName());
    serializer.write(object, output);
    return output;
  }

  /**
   * Reads an object from the given input stream.
   * <p>
   * During deserialization, the buffer will first be read to determine the type to be deserialized.
   * If the object was written using a serializable type ID, the given ID will be used
   * to locate the serialized type. The type must have been
   * {@link Serializer#register(Class) registered} with this {@link Serializer} instance
   * in order to perform a reverse lookup.
   * <p>
   * If the type was written to the buffer with a fully qualified class name,
   * the class name will be used to load the object class via {@link Class#forName(String)}.
   * Serializable types must implement a no-argument constructor to be properly deserialized.
   * <p>
   * If the serialized type is an instance of {@link MessagingSerializable},
   * {@link MessagingSerializable#readObject(DataInputStream)} will be used to
   * read the object attributes from the buffer.
   * <p>
   * If the type is a {@link java.io.Serializable} type serialized
   * with native Java serialization, it will be read from the buffer
   * via {@link java.io.ObjectInputStream}.
   * <p>
   *
   * @param inputStream The input stream from which to read the object
   * @param <T> The object type
   * @return The read object
   * @throws MessagingException If no type could be read from the provided buffer
   */
  public <T> T readObject(InputStream inputStream) throws MessagingException {
    try {
      return readObject(new DataInputStream(inputStream));
    } catch (IOException | ClassNotFoundException e) {
      throw new MessagingException("Failed to read object", e);
    }
  }

  /**
   * Reads an object from the given input stream.
   * During deserialization, the buffer will first be read to determine the type to be deserialized.
   * If the object was written using a serializable type ID, the given ID will be used
   * to locate the serialized type. The type must have been
   * {@link Serializer#register(Class) registered} with this {@link Serializer} instance
   * in order to perform a reverse lookup.
   * <p>
   * If the type was written to the buffer with a fully qualified class name, the class name
   * will be used to load the object class via {@link Class#forName(String)}.
   * Serializable types must implement a no-argument constructor to be properly deserialized.
   * <p>
   * If the serialized type is an instance of {@link MessagingSerializable},
   * {@link MessagingSerializable#readObject(DataInputStream)} will be used to
   * read the object attributes from the buffer.
   * <p>
   * If the type is a {@link java.io.Serializable} type serialized with native Java serialization,
   * it will be read from the buffer via {@link java.io.ObjectInputStream}.
   * <p>
   *
   * @param input The stream from which to read the object
   * @param <T> The object type
   * @return The read object
   */
  @SuppressWarnings("unchecked")
  public <T> T readObject(DataInputStream input) throws IOException, ClassNotFoundException {
    int code = input.readByte();
    Identifier identifier = Identifier.forCode(code);
    switch (identifier) {
      case NULL:
        return null;
      case CLASS:
        return readByClass(input);
      default:
        return readById(identifier.read(input), input);
    }
  }

  /**
   * Reads a serializable object.
   *
   * @param id The serializable type ID
   * @param input the data input stream to read from
   * @param <T> The object type
   * @return The read object
   */
  @SuppressWarnings("unchecked")
  private <T> T readById(int id, DataInputStream input) throws IOException, ClassNotFoundException {
    Class<T> type = (Class<T>) mRegistry.type(id);
    if (type == null) {
      throw new MessagingException("cannot deserialize: unknown type");
    }
    TypeSerializer<T> serializer = getSerializer(type);
    if (serializer == null) {
      throw new MessagingException("cannot deserialize: unknown type");
    }
    return serializer.read(type, input);
  }

  /**
   * Reads a writable object.
   *
   * @param input The input from which to read the object
   * @param <T> The object type
   * @return The read object
   */
  @SuppressWarnings("unchecked")
  private <T> T readByClass(DataInputStream input) throws IOException, ClassNotFoundException {
    String name = SerializerUtils.readStringFromStream(input);

    Class<T> type = (Class<T>) mTypes.get(name);
    if (type == null) {
      try {
        type = (Class<T>) Class.forName(name);
        if (type == null) {
          throw new MessagingException("Cannot deserialize: unknown type");
        }
        mTypes.put(name, type);
      } catch (ClassNotFoundException e) {
        throw new MessagingException("Object class not found: " + name, e);
      }
    }

    TypeSerializer<T> serializer = getSerializer(type);
    if (serializer == null) {
      throw new MessagingException("Cannot deserialize unregistered type: " + name);
    }
    return serializer.read(type, input);
  }

  /**
   * Resolves serializable types with the given resolver.
   * <p>
   * This allows users to modify the serializable types registered
   * to an existing {@link Serializer} instance. Types resolved by the provided resolver(s)
   * will be added to existing types resolved by any type resolvers provided
   * to this object's constructor or by previous calls to this method.
   *
   * @param resolvers The resolvers with which to resolve serializable types
   * @return The serializer instance
   */
  public Serializer resolve(SerializableTypeResolver... resolvers) {
    mRegistry.resolve(resolvers);
    return this;
  }

  /**
   * Returns the serializer for the given type else {@code null} if no serializer
   * or factory are registered for the {@code type}.
   */
  @SuppressWarnings("unchecked")
  private <T> TypeSerializer<T> getSerializer(Class<T> type) {
    TypeSerializer<T> serializer = (TypeSerializer<T>) mSerializers.get(type);
    if (serializer == null) {
      TypeSerializerFactory factory = mRegistry.factory(type);
      if (factory != null) {
        serializer = (TypeSerializer<T>) factory.createSerializer(type);
        mSerializers.put(type, serializer);
      }
    }
    return serializer;
  }
}
