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
    comments = "Source: common.proto")
public final class AlluxioServiceGrpc {

  private AlluxioServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.AlluxioService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetServiceVersionMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetServiceVersionPOptions,
      alluxio.grpc.GetServiceVersionPResponse> METHOD_GET_SERVICE_VERSION = getGetServiceVersionMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetServiceVersionPOptions,
      alluxio.grpc.GetServiceVersionPResponse> getGetServiceVersionMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetServiceVersionPOptions,
      alluxio.grpc.GetServiceVersionPResponse> getGetServiceVersionMethod() {
    return getGetServiceVersionMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetServiceVersionPOptions,
      alluxio.grpc.GetServiceVersionPResponse> getGetServiceVersionMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetServiceVersionPOptions, alluxio.grpc.GetServiceVersionPResponse> getGetServiceVersionMethod;
    if ((getGetServiceVersionMethod = AlluxioServiceGrpc.getGetServiceVersionMethod) == null) {
      synchronized (AlluxioServiceGrpc.class) {
        if ((getGetServiceVersionMethod = AlluxioServiceGrpc.getGetServiceVersionMethod) == null) {
          AlluxioServiceGrpc.getGetServiceVersionMethod = getGetServiceVersionMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetServiceVersionPOptions, alluxio.grpc.GetServiceVersionPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.AlluxioService", "getServiceVersion"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetServiceVersionPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetServiceVersionPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new AlluxioServiceMethodDescriptorSupplier("getServiceVersion"))
                  .build();
          }
        }
     }
     return getGetServiceVersionMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AlluxioServiceStub newStub(io.grpc.Channel channel) {
    return new AlluxioServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AlluxioServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new AlluxioServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AlluxioServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new AlluxioServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class AlluxioServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Returns the version of the master service.
     * NOTE: The version should be updated every time a backwards incompatible API change occurs.
     * </pre>
     */
    public void getServiceVersion(alluxio.grpc.GetServiceVersionPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetServiceVersionPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetServiceVersionMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetServiceVersionMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetServiceVersionPOptions,
                alluxio.grpc.GetServiceVersionPResponse>(
                  this, METHODID_GET_SERVICE_VERSION)))
          .build();
    }
  }

  /**
   */
  public static final class AlluxioServiceStub extends io.grpc.stub.AbstractStub<AlluxioServiceStub> {
    private AlluxioServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AlluxioServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AlluxioServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AlluxioServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the version of the master service.
     * NOTE: The version should be updated every time a backwards incompatible API change occurs.
     * </pre>
     */
    public void getServiceVersion(alluxio.grpc.GetServiceVersionPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetServiceVersionPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetServiceVersionMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class AlluxioServiceBlockingStub extends io.grpc.stub.AbstractStub<AlluxioServiceBlockingStub> {
    private AlluxioServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AlluxioServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AlluxioServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AlluxioServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the version of the master service.
     * NOTE: The version should be updated every time a backwards incompatible API change occurs.
     * </pre>
     */
    public alluxio.grpc.GetServiceVersionPResponse getServiceVersion(alluxio.grpc.GetServiceVersionPOptions request) {
      return blockingUnaryCall(
          getChannel(), getGetServiceVersionMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class AlluxioServiceFutureStub extends io.grpc.stub.AbstractStub<AlluxioServiceFutureStub> {
    private AlluxioServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AlluxioServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AlluxioServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AlluxioServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the version of the master service.
     * NOTE: The version should be updated every time a backwards incompatible API change occurs.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetServiceVersionPResponse> getServiceVersion(
        alluxio.grpc.GetServiceVersionPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getGetServiceVersionMethodHelper(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_SERVICE_VERSION = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AlluxioServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(AlluxioServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_SERVICE_VERSION:
          serviceImpl.getServiceVersion((alluxio.grpc.GetServiceVersionPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetServiceVersionPResponse>) responseObserver);
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

  private static abstract class AlluxioServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AlluxioServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.CommonProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("AlluxioService");
    }
  }

  private static final class AlluxioServiceFileDescriptorSupplier
      extends AlluxioServiceBaseDescriptorSupplier {
    AlluxioServiceFileDescriptorSupplier() {}
  }

  private static final class AlluxioServiceMethodDescriptorSupplier
      extends AlluxioServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    AlluxioServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (AlluxioServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AlluxioServiceFileDescriptorSupplier())
              .addMethod(getGetServiceVersionMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
