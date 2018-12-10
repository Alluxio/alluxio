package alluxio.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.10.1)",
    comments = "Source: key_value_worker.proto")
public final class KeyValueWorkerClientServiceGrpc {

  private KeyValueWorkerClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.KeyValueWorkerClientService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetPRequest,
      alluxio.grpc.GetPResponse> METHOD_GET = getGetMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetPRequest,
      alluxio.grpc.GetPResponse> getGetMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetPRequest,
      alluxio.grpc.GetPResponse> getGetMethod() {
    return getGetMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetPRequest,
      alluxio.grpc.GetPResponse> getGetMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetPRequest, alluxio.grpc.GetPResponse> getGetMethod;
    if ((getGetMethod = KeyValueWorkerClientServiceGrpc.getGetMethod) == null) {
      synchronized (KeyValueWorkerClientServiceGrpc.class) {
        if ((getGetMethod = KeyValueWorkerClientServiceGrpc.getGetMethod) == null) {
          KeyValueWorkerClientServiceGrpc.getGetMethod = getGetMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetPRequest, alluxio.grpc.GetPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.KeyValueWorkerClientService", "Get"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new KeyValueWorkerClientServiceMethodDescriptorSupplier("Get"))
                  .build();
          }
        }
     }
     return getGetMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetNextKeysMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetNextKeysPRequest,
      alluxio.grpc.GetNextKeysPResponse> METHOD_GET_NEXT_KEYS = getGetNextKeysMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetNextKeysPRequest,
      alluxio.grpc.GetNextKeysPResponse> getGetNextKeysMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetNextKeysPRequest,
      alluxio.grpc.GetNextKeysPResponse> getGetNextKeysMethod() {
    return getGetNextKeysMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetNextKeysPRequest,
      alluxio.grpc.GetNextKeysPResponse> getGetNextKeysMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetNextKeysPRequest, alluxio.grpc.GetNextKeysPResponse> getGetNextKeysMethod;
    if ((getGetNextKeysMethod = KeyValueWorkerClientServiceGrpc.getGetNextKeysMethod) == null) {
      synchronized (KeyValueWorkerClientServiceGrpc.class) {
        if ((getGetNextKeysMethod = KeyValueWorkerClientServiceGrpc.getGetNextKeysMethod) == null) {
          KeyValueWorkerClientServiceGrpc.getGetNextKeysMethod = getGetNextKeysMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetNextKeysPRequest, alluxio.grpc.GetNextKeysPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.KeyValueWorkerClientService", "GetNextKeys"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetNextKeysPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetNextKeysPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new KeyValueWorkerClientServiceMethodDescriptorSupplier("GetNextKeys"))
                  .build();
          }
        }
     }
     return getGetNextKeysMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetSizeMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetSizePRequest,
      alluxio.grpc.GetSizePResponse> METHOD_GET_SIZE = getGetSizeMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetSizePRequest,
      alluxio.grpc.GetSizePResponse> getGetSizeMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetSizePRequest,
      alluxio.grpc.GetSizePResponse> getGetSizeMethod() {
    return getGetSizeMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetSizePRequest,
      alluxio.grpc.GetSizePResponse> getGetSizeMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetSizePRequest, alluxio.grpc.GetSizePResponse> getGetSizeMethod;
    if ((getGetSizeMethod = KeyValueWorkerClientServiceGrpc.getGetSizeMethod) == null) {
      synchronized (KeyValueWorkerClientServiceGrpc.class) {
        if ((getGetSizeMethod = KeyValueWorkerClientServiceGrpc.getGetSizeMethod) == null) {
          KeyValueWorkerClientServiceGrpc.getGetSizeMethod = getGetSizeMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetSizePRequest, alluxio.grpc.GetSizePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.KeyValueWorkerClientService", "GetSize"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetSizePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetSizePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new KeyValueWorkerClientServiceMethodDescriptorSupplier("GetSize"))
                  .build();
          }
        }
     }
     return getGetSizeMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static KeyValueWorkerClientServiceStub newStub(io.grpc.Channel channel) {
    return new KeyValueWorkerClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static KeyValueWorkerClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new KeyValueWorkerClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static KeyValueWorkerClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new KeyValueWorkerClientServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class KeyValueWorkerClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Looks up a key in the block with the given block id.
     * </pre>
     */
    public void get(alluxio.grpc.GetPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Gets a batch of keys next to the given key in the partition.
     * If current key is null, it means get the initial batch of keys.
     * If there are no more next keys, an empty list is returned.
     * </pre>
     */
    public void getNextKeys(alluxio.grpc.GetNextKeysPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetNextKeysPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetNextKeysMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Gets the number of key-value pairs in the partition.
     * </pre>
     */
    public void getSize(alluxio.grpc.GetSizePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetSizePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetSizeMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetPRequest,
                alluxio.grpc.GetPResponse>(
                  this, METHODID_GET)))
          .addMethod(
            getGetNextKeysMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetNextKeysPRequest,
                alluxio.grpc.GetNextKeysPResponse>(
                  this, METHODID_GET_NEXT_KEYS)))
          .addMethod(
            getGetSizeMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetSizePRequest,
                alluxio.grpc.GetSizePResponse>(
                  this, METHODID_GET_SIZE)))
          .build();
    }
  }

  /**
   */
  public static final class KeyValueWorkerClientServiceStub extends io.grpc.stub.AbstractStub<KeyValueWorkerClientServiceStub> {
    private KeyValueWorkerClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private KeyValueWorkerClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KeyValueWorkerClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new KeyValueWorkerClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Looks up a key in the block with the given block id.
     * </pre>
     */
    public void get(alluxio.grpc.GetPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Gets a batch of keys next to the given key in the partition.
     * If current key is null, it means get the initial batch of keys.
     * If there are no more next keys, an empty list is returned.
     * </pre>
     */
    public void getNextKeys(alluxio.grpc.GetNextKeysPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetNextKeysPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetNextKeysMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Gets the number of key-value pairs in the partition.
     * </pre>
     */
    public void getSize(alluxio.grpc.GetSizePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetSizePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetSizeMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class KeyValueWorkerClientServiceBlockingStub extends io.grpc.stub.AbstractStub<KeyValueWorkerClientServiceBlockingStub> {
    private KeyValueWorkerClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private KeyValueWorkerClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KeyValueWorkerClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new KeyValueWorkerClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Looks up a key in the block with the given block id.
     * </pre>
     */
    public alluxio.grpc.GetPResponse get(alluxio.grpc.GetPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Gets a batch of keys next to the given key in the partition.
     * If current key is null, it means get the initial batch of keys.
     * If there are no more next keys, an empty list is returned.
     * </pre>
     */
    public alluxio.grpc.GetNextKeysPResponse getNextKeys(alluxio.grpc.GetNextKeysPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetNextKeysMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Gets the number of key-value pairs in the partition.
     * </pre>
     */
    public alluxio.grpc.GetSizePResponse getSize(alluxio.grpc.GetSizePRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetSizeMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class KeyValueWorkerClientServiceFutureStub extends io.grpc.stub.AbstractStub<KeyValueWorkerClientServiceFutureStub> {
    private KeyValueWorkerClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private KeyValueWorkerClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KeyValueWorkerClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new KeyValueWorkerClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Looks up a key in the block with the given block id.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetPResponse> get(
        alluxio.grpc.GetPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Gets a batch of keys next to the given key in the partition.
     * If current key is null, it means get the initial batch of keys.
     * If there are no more next keys, an empty list is returned.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetNextKeysPResponse> getNextKeys(
        alluxio.grpc.GetNextKeysPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetNextKeysMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Gets the number of key-value pairs in the partition.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetSizePResponse> getSize(
        alluxio.grpc.GetSizePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetSizeMethodHelper(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET = 0;
  private static final int METHODID_GET_NEXT_KEYS = 1;
  private static final int METHODID_GET_SIZE = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final KeyValueWorkerClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(KeyValueWorkerClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET:
          serviceImpl.get((alluxio.grpc.GetPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetPResponse>) responseObserver);
          break;
        case METHODID_GET_NEXT_KEYS:
          serviceImpl.getNextKeys((alluxio.grpc.GetNextKeysPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetNextKeysPResponse>) responseObserver);
          break;
        case METHODID_GET_SIZE:
          serviceImpl.getSize((alluxio.grpc.GetSizePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetSizePResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class KeyValueWorkerClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    KeyValueWorkerClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.KeyValueWorkerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("KeyValueWorkerClientService");
    }
  }

  private static final class KeyValueWorkerClientServiceFileDescriptorSupplier
      extends KeyValueWorkerClientServiceBaseDescriptorSupplier {
    KeyValueWorkerClientServiceFileDescriptorSupplier() {}
  }

  private static final class KeyValueWorkerClientServiceMethodDescriptorSupplier
      extends KeyValueWorkerClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    KeyValueWorkerClientServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (KeyValueWorkerClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new KeyValueWorkerClientServiceFileDescriptorSupplier())
              .addMethod(getGetMethodHelper())
              .addMethod(getGetNextKeysMethodHelper())
              .addMethod(getGetSizeMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
