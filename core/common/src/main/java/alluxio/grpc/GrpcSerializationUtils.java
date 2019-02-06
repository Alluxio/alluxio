/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.grpc;

import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.internal.ReadableBuffer;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utilities for gRPC message serialization.
 */
public class GrpcSerializationUtils {
  public static final CallOptions.Key<DataMessageMarshaller> RESPONSE_MARSHALLER =
      CallOptions.Key.create("response marshaller");

  private static final Logger LOG = LoggerFactory.getLogger(GrpcSerializationUtils.class);

  private static final int TAG_TYPE_BITS = 3;
  private static Constructor<?> sNettyWritableBufferConstruct;
  private static Field sBufferList;
  private static Field sCurrent;
  private static Field sReadableBufferField = null;
  private static boolean sZeroCopySendSupported = true;
  private static boolean sZeroCopyReceiveSupported = true;

  static {
    try {
      sReadableBufferField = getPrivateField(
          "io.grpc.internal.ReadableBuffers$BufferInputStream", "buffer");
    } catch (Exception e) {
      LOG.warn("Cannot get gRPC input stream buffer, zero copy send will be disabled.", e);
      sZeroCopySendSupported = false;
    }

    try {
      sNettyWritableBufferConstruct = getPrivateConstructor(
          "io.grpc.netty.NettyWritableBuffer", ByteBuf.class);
      sBufferList = getPrivateField(
          "io.grpc.internal.MessageFramer$BufferChainOutputStream", "bufferList");
      sCurrent = getPrivateField(
          "io.grpc.internal.MessageFramer$BufferChainOutputStream", "current");
    } catch (Exception e) {
      LOG.warn("Cannot get gRPC output stream buffer, zero copy receive will be disabled.", e);
      sZeroCopyReceiveSupported = false;
    }
  }

  private static Field getPrivateField(String className, String fieldName)
      throws NoSuchFieldException, ClassNotFoundException {
    Class<?> declaringClass = Class.forName(className);
    Field field = declaringClass.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }

  private static Constructor<?> getPrivateConstructor(String className, Class<?> ...parameterTypes)
      throws ClassNotFoundException, NoSuchMethodException {
    Class<?> declaringClass = Class.forName(className);
    Constructor<?> constructor = declaringClass.getDeclaredConstructor(parameterTypes);
    constructor.setAccessible(true);
    return constructor;
  }

  /**
   * Makes a gRPC tag for a field.
   *
   * @param fieldNumber field number
   * @param wireType wire type of the field
   * @return the gRPC tag
   */
  public static int makeTag(final int fieldNumber, final int wireType) {
    // This is a public version of WireFormat.makeTag.
    return (fieldNumber << TAG_TYPE_BITS) | wireType;
  }

  /**
   * Gets a buffer directly from a gRPC input stream.
   *
   * @param stream the input stream
   * @return the raw data buffer
   */
  public static ReadableBuffer getBufferFromStream(InputStream stream) {
    if (!sZeroCopyReceiveSupported
        || !stream.getClass().equals(sReadableBufferField.getDeclaringClass())) {
      return null;
    }
    try {
      return (ReadableBuffer) sReadableBufferField.get(stream);
    } catch (Exception e) {
      LOG.warn("Failed to get data buffer from stream.", e);
      return null;
    }
  }

  /**
   * Add the given buffers directly to the gRPC output stream.
   *
   * @param buffers the buffers to be added
   * @param stream the output stream
   * @return whether the buffers are added successfully
   */
  public static boolean addBuffersToStream(ByteBuf[] buffers, OutputStream stream) {
    if (!sZeroCopySendSupported || !stream.getClass().equals(sBufferList.getDeclaringClass())) {
      return false;
    }
    try {
      if (sCurrent.get(stream) != null) {
        return false;
      }
      for (ByteBuf buffer : buffers) {
        Object nettyBuffer = sNettyWritableBufferConstruct.newInstance(buffer);
        List list = (List) sBufferList.get(stream);
        list.add(nettyBuffer);
        buffer.retain();
        sCurrent.set(stream, nettyBuffer);
      }
      return true;
    } catch (Exception e) {
      LOG.warn("Failed to add data buffer to stream.", e);
      return false;
    }
  }

  /**
   * Creates a service definition that uses custom marshallers.
   *
   * @param service the service to intercept
   * @param marshallers a map that specifies which marshaller to use for each method
   * @return the new service definition
   */
  public static ServerServiceDefinition overrideMethods(
      final ServerServiceDefinition service,
      final Map<MethodDescriptor, MethodDescriptor> marshallers) {
    List<ServerMethodDefinition<?, ?>> newMethods = new ArrayList<ServerMethodDefinition<?, ?>>();
    List<MethodDescriptor<?, ?>> newDescriptors = new ArrayList<MethodDescriptor<?, ?>>();
    // intercepts the descriptors
    for (final ServerMethodDefinition<?, ?> definition : service.getMethods()) {
      ServerMethodDefinition<?, ?> newMethod = interceptMethod(definition, marshallers);
      newDescriptors.add(newMethod.getMethodDescriptor());
      newMethods.add(newMethod);
    }
    // builds the new service descriptor
    final ServerServiceDefinition.Builder serviceBuilder = ServerServiceDefinition
        .builder(new ServiceDescriptor(service.getServiceDescriptor().getName(), newDescriptors));
    // creates the new service definition
    for (ServerMethodDefinition<?, ?> definition : newMethods) {
      serviceBuilder.addMethod(definition);
    }
    return serviceBuilder.build();
  }

  private static <ReqT, RespT>  ServerMethodDefinition<ReqT, RespT> interceptMethod(
      final ServerMethodDefinition<ReqT, RespT> definition,
      final Map<MethodDescriptor, MethodDescriptor> newMethods) {
    MethodDescriptor<ReqT, RespT> descriptor = definition.getMethodDescriptor();
    MethodDescriptor newMethod = newMethods.get(descriptor);
    if (newMethod != null) {
      return ServerMethodDefinition.create(newMethod, definition.getServerCallHandler());
    }
    return definition;
  }
}
