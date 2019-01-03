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
public final class ServiceVersionClientServiceGrpc {

  private ServiceVersionClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.ServiceVersionClientService";

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
    if ((getGetServiceVersionMethod = ServiceVersionClientServiceGrpc.getGetServiceVersionMethod) == null) {
      synchronized (ServiceVersionClientServiceGrpc.class) {
        if ((getGetServiceVersionMethod = ServiceVersionClientServiceGrpc.getGetServiceVersionMethod) == null) {
          ServiceVersionClientServiceGrpc.getGetServiceVersionMethod = getGetServiceVersionMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetServiceVersionPRequest, alluxio.grpc.GetServiceVersionPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.ServiceVersionClientService", "getServiceVersion"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetServiceVersionPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetServiceVersionPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new ServiceVersionClientServiceMethodDescriptorSupplier("getServiceVersion"))
                  .build();
          }
        }
     }
     return getGetServiceVersionMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ServiceVersionClientServiceStub newStub(io.grpc.Channel channel) {
    return new ServiceVersionClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ServiceVersionClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ServiceVersionClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ServiceVersionClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ServiceVersionClientServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class ServiceVersionClientServiceImplBase implements io.grpc.BindableService {

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
  public static final class ServiceVersionClientServiceStub extends io.grpc.stub.AbstractStub<ServiceVersionClientServiceStub> {
    private ServiceVersionClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ServiceVersionClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ServiceVersionClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ServiceVersionClientServiceStub(channel, callOptions);
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
  public static final class ServiceVersionClientServiceBlockingStub extends io.grpc.stub.AbstractStub<ServiceVersionClientServiceBlockingStub> {
    private ServiceVersionClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ServiceVersionClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ServiceVersionClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ServiceVersionClientServiceBlockingStub(channel, callOptions);
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
  public static final class ServiceVersionClientServiceFutureStub extends io.grpc.stub.AbstractStub<ServiceVersionClientServiceFutureStub> {
    private ServiceVersionClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ServiceVersionClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ServiceVersionClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ServiceVersionClientServiceFutureStub(channel, callOptions);
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
    private final ServiceVersionClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ServiceVersionClientServiceImplBase serviceImpl, int methodId) {
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

  private static abstract class ServiceVersionClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ServiceVersionClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.VersionProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ServiceVersionClientService");
    }
  }

  private static final class ServiceVersionClientServiceFileDescriptorSupplier
      extends ServiceVersionClientServiceBaseDescriptorSupplier {
    ServiceVersionClientServiceFileDescriptorSupplier() {}
  }

  private static final class ServiceVersionClientServiceMethodDescriptorSupplier
      extends ServiceVersionClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ServiceVersionClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ServiceVersionClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ServiceVersionClientServiceFileDescriptorSupplier())
              .addMethod(getGetServiceVersionMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
