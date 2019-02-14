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
 * <pre>
 **
 * This interface contains meta master service endpoints for Alluxio clients
 * to query cluster configuration.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: grpc/meta_master.proto")
public final class MetaMasterConfigurationServiceGrpc {

  private MetaMasterConfigurationServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.meta.MetaMasterConfigurationService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetConfigurationPOptions,
      alluxio.grpc.GetConfigurationPResponse> getGetConfigurationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetConfiguration",
      requestType = alluxio.grpc.GetConfigurationPOptions.class,
      responseType = alluxio.grpc.GetConfigurationPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetConfigurationPOptions,
      alluxio.grpc.GetConfigurationPResponse> getGetConfigurationMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetConfigurationPOptions, alluxio.grpc.GetConfigurationPResponse> getGetConfigurationMethod;
    if ((getGetConfigurationMethod = MetaMasterConfigurationServiceGrpc.getGetConfigurationMethod) == null) {
      synchronized (MetaMasterConfigurationServiceGrpc.class) {
        if ((getGetConfigurationMethod = MetaMasterConfigurationServiceGrpc.getGetConfigurationMethod) == null) {
          MetaMasterConfigurationServiceGrpc.getGetConfigurationMethod = getGetConfigurationMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetConfigurationPOptions, alluxio.grpc.GetConfigurationPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.meta.MetaMasterConfigurationService", "GetConfiguration"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetConfigurationPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetConfigurationPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new MetaMasterConfigurationServiceMethodDescriptorSupplier("GetConfiguration"))
                  .build();
          }
        }
     }
     return getGetConfigurationMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MetaMasterConfigurationServiceStub newStub(io.grpc.Channel channel) {
    return new MetaMasterConfigurationServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetaMasterConfigurationServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new MetaMasterConfigurationServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MetaMasterConfigurationServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new MetaMasterConfigurationServiceFutureStub(channel);
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients
   * to query cluster configuration.
   * </pre>
   */
  public static abstract class MetaMasterConfigurationServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Returns a list of Alluxio runtime configuration information.
     * </pre>
     */
    public void getConfiguration(alluxio.grpc.GetConfigurationPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigurationPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetConfigurationMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetConfigurationMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetConfigurationPOptions,
                alluxio.grpc.GetConfigurationPResponse>(
                  this, METHODID_GET_CONFIGURATION)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients
   * to query cluster configuration.
   * </pre>
   */
  public static final class MetaMasterConfigurationServiceStub extends io.grpc.stub.AbstractStub<MetaMasterConfigurationServiceStub> {
    private MetaMasterConfigurationServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaMasterConfigurationServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterConfigurationServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaMasterConfigurationServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns a list of Alluxio runtime configuration information.
     * </pre>
     */
    public void getConfiguration(alluxio.grpc.GetConfigurationPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigurationPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetConfigurationMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients
   * to query cluster configuration.
   * </pre>
   */
  public static final class MetaMasterConfigurationServiceBlockingStub extends io.grpc.stub.AbstractStub<MetaMasterConfigurationServiceBlockingStub> {
    private MetaMasterConfigurationServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaMasterConfigurationServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterConfigurationServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaMasterConfigurationServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns a list of Alluxio runtime configuration information.
     * </pre>
     */
    public alluxio.grpc.GetConfigurationPResponse getConfiguration(alluxio.grpc.GetConfigurationPOptions request) {
      return blockingUnaryCall(
          getChannel(), getGetConfigurationMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients
   * to query cluster configuration.
   * </pre>
   */
  public static final class MetaMasterConfigurationServiceFutureStub extends io.grpc.stub.AbstractStub<MetaMasterConfigurationServiceFutureStub> {
    private MetaMasterConfigurationServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaMasterConfigurationServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterConfigurationServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaMasterConfigurationServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns a list of Alluxio runtime configuration information.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetConfigurationPResponse> getConfiguration(
        alluxio.grpc.GetConfigurationPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getGetConfigurationMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_CONFIGURATION = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MetaMasterConfigurationServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(MetaMasterConfigurationServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_CONFIGURATION:
          serviceImpl.getConfiguration((alluxio.grpc.GetConfigurationPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigurationPResponse>) responseObserver);
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

  private static abstract class MetaMasterConfigurationServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MetaMasterConfigurationServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.MetaMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("MetaMasterConfigurationService");
    }
  }

  private static final class MetaMasterConfigurationServiceFileDescriptorSupplier
      extends MetaMasterConfigurationServiceBaseDescriptorSupplier {
    MetaMasterConfigurationServiceFileDescriptorSupplier() {}
  }

  private static final class MetaMasterConfigurationServiceMethodDescriptorSupplier
      extends MetaMasterConfigurationServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    MetaMasterConfigurationServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (MetaMasterConfigurationServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MetaMasterConfigurationServiceFileDescriptorSupplier())
              .addMethod(getGetConfigurationMethod())
              .build();
        }
      }
    }
    return result;
  }
}
