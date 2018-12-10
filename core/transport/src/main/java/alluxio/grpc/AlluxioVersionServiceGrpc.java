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
    comments = "Source: version.proto")
public final class AlluxioVersionServiceGrpc {

  private AlluxioVersionServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.AlluxioVersionService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetServiceVersionMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetServiceVersionPRequest,
      alluxio.grpc.GetServiceVersionPResponse> METHOD_GET_SERVICE_VERSION = getGetServiceVersionMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetServiceVersionPRequest,
      alluxio.grpc.GetServiceVersionPResponse> getGetServiceVersionMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetServiceVersionPRequest,
      alluxio.grpc.GetServiceVersionPResponse> getGetServiceVersionMethod() {
    return getGetServiceVersionMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetServiceVersionPRequest,
      alluxio.grpc.GetServiceVersionPResponse> getGetServiceVersionMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetServiceVersionPRequest, alluxio.grpc.GetServiceVersionPResponse> getGetServiceVersionMethod;
    if ((getGetServiceVersionMethod = AlluxioVersionServiceGrpc.getGetServiceVersionMethod) == null) {
      synchronized (AlluxioVersionServiceGrpc.class) {
        if ((getGetServiceVersionMethod = AlluxioVersionServiceGrpc.getGetServiceVersionMethod) == null) {
          AlluxioVersionServiceGrpc.getGetServiceVersionMethod = getGetServiceVersionMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetServiceVersionPRequest, alluxio.grpc.GetServiceVersionPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.AlluxioVersionService", "getServiceVersion"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetServiceVersionPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetServiceVersionPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new AlluxioVersionServiceMethodDescriptorSupplier("getServiceVersion"))
                  .build();
          }
        }
     }
     return getGetServiceVersionMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AlluxioVersionServiceStub newStub(io.grpc.Channel channel) {
    return new AlluxioVersionServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AlluxioVersionServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new AlluxioVersionServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AlluxioVersionServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new AlluxioVersionServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class AlluxioVersionServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Returns the version of the master service.
     * NOTE: The version should be updated every time a backwards incompatible API change occurs.
     * </pre>
     */
    public void getServiceVersion(alluxio.grpc.GetServiceVersionPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetServiceVersionPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetServiceVersionMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetServiceVersionMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetServiceVersionPRequest,
                alluxio.grpc.GetServiceVersionPResponse>(
                  this, METHODID_GET_SERVICE_VERSION)))
          .build();
    }
  }

  /**
   */
  public static final class AlluxioVersionServiceStub extends io.grpc.stub.AbstractStub<AlluxioVersionServiceStub> {
    private AlluxioVersionServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AlluxioVersionServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AlluxioVersionServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AlluxioVersionServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the version of the master service.
     * NOTE: The version should be updated every time a backwards incompatible API change occurs.
     * </pre>
     */
    public void getServiceVersion(alluxio.grpc.GetServiceVersionPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetServiceVersionPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetServiceVersionMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class AlluxioVersionServiceBlockingStub extends io.grpc.stub.AbstractStub<AlluxioVersionServiceBlockingStub> {
    private AlluxioVersionServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AlluxioVersionServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AlluxioVersionServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AlluxioVersionServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the version of the master service.
     * NOTE: The version should be updated every time a backwards incompatible API change occurs.
     * </pre>
     */
    public alluxio.grpc.GetServiceVersionPResponse getServiceVersion(alluxio.grpc.GetServiceVersionPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetServiceVersionMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class AlluxioVersionServiceFutureStub extends io.grpc.stub.AbstractStub<AlluxioVersionServiceFutureStub> {
    private AlluxioVersionServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AlluxioVersionServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AlluxioVersionServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AlluxioVersionServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the version of the master service.
     * NOTE: The version should be updated every time a backwards incompatible API change occurs.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetServiceVersionPResponse> getServiceVersion(
        alluxio.grpc.GetServiceVersionPRequest request) {
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
    private final AlluxioVersionServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(AlluxioVersionServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_SERVICE_VERSION:
          serviceImpl.getServiceVersion((alluxio.grpc.GetServiceVersionPRequest) request,
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

  private static abstract class AlluxioVersionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AlluxioVersionServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.VersionProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("AlluxioVersionService");
    }
  }

  private static final class AlluxioVersionServiceFileDescriptorSupplier
      extends AlluxioVersionServiceBaseDescriptorSupplier {
    AlluxioVersionServiceFileDescriptorSupplier() {}
  }

  private static final class AlluxioVersionServiceMethodDescriptorSupplier
      extends AlluxioVersionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    AlluxioVersionServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (AlluxioVersionServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AlluxioVersionServiceFileDescriptorSupplier())
              .addMethod(getGetServiceVersionMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
