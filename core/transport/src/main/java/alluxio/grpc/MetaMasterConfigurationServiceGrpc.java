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

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.SetPathConfigurationPRequest,
      alluxio.grpc.SetPathConfigurationPResponse> getSetPathConfigurationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetPathConfiguration",
      requestType = alluxio.grpc.SetPathConfigurationPRequest.class,
      responseType = alluxio.grpc.SetPathConfigurationPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.SetPathConfigurationPRequest,
      alluxio.grpc.SetPathConfigurationPResponse> getSetPathConfigurationMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.SetPathConfigurationPRequest, alluxio.grpc.SetPathConfigurationPResponse> getSetPathConfigurationMethod;
    if ((getSetPathConfigurationMethod = MetaMasterConfigurationServiceGrpc.getSetPathConfigurationMethod) == null) {
      synchronized (MetaMasterConfigurationServiceGrpc.class) {
        if ((getSetPathConfigurationMethod = MetaMasterConfigurationServiceGrpc.getSetPathConfigurationMethod) == null) {
          MetaMasterConfigurationServiceGrpc.getSetPathConfigurationMethod = getSetPathConfigurationMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.SetPathConfigurationPRequest, alluxio.grpc.SetPathConfigurationPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.meta.MetaMasterConfigurationService", "SetPathConfiguration"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SetPathConfigurationPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SetPathConfigurationPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new MetaMasterConfigurationServiceMethodDescriptorSupplier("SetPathConfiguration"))
                  .build();
          }
        }
     }
     return getSetPathConfigurationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RemovePathConfigurationPRequest,
      alluxio.grpc.RemovePathConfigurationPResponse> getRemovePathConfigurationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RemovePathConfiguration",
      requestType = alluxio.grpc.RemovePathConfigurationPRequest.class,
      responseType = alluxio.grpc.RemovePathConfigurationPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.RemovePathConfigurationPRequest,
      alluxio.grpc.RemovePathConfigurationPResponse> getRemovePathConfigurationMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.RemovePathConfigurationPRequest, alluxio.grpc.RemovePathConfigurationPResponse> getRemovePathConfigurationMethod;
    if ((getRemovePathConfigurationMethod = MetaMasterConfigurationServiceGrpc.getRemovePathConfigurationMethod) == null) {
      synchronized (MetaMasterConfigurationServiceGrpc.class) {
        if ((getRemovePathConfigurationMethod = MetaMasterConfigurationServiceGrpc.getRemovePathConfigurationMethod) == null) {
          MetaMasterConfigurationServiceGrpc.getRemovePathConfigurationMethod = getRemovePathConfigurationMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.RemovePathConfigurationPRequest, alluxio.grpc.RemovePathConfigurationPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.meta.MetaMasterConfigurationService", "RemovePathConfiguration"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RemovePathConfigurationPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RemovePathConfigurationPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new MetaMasterConfigurationServiceMethodDescriptorSupplier("RemovePathConfiguration"))
                  .build();
          }
        }
     }
     return getRemovePathConfigurationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetConfigVersionPOptions,
      alluxio.grpc.GetConfigVersionPResponse> getGetConfigVersionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetConfigVersion",
      requestType = alluxio.grpc.GetConfigVersionPOptions.class,
      responseType = alluxio.grpc.GetConfigVersionPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetConfigVersionPOptions,
      alluxio.grpc.GetConfigVersionPResponse> getGetConfigVersionMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetConfigVersionPOptions, alluxio.grpc.GetConfigVersionPResponse> getGetConfigVersionMethod;
    if ((getGetConfigVersionMethod = MetaMasterConfigurationServiceGrpc.getGetConfigVersionMethod) == null) {
      synchronized (MetaMasterConfigurationServiceGrpc.class) {
        if ((getGetConfigVersionMethod = MetaMasterConfigurationServiceGrpc.getGetConfigVersionMethod) == null) {
          MetaMasterConfigurationServiceGrpc.getGetConfigVersionMethod = getGetConfigVersionMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetConfigVersionPOptions, alluxio.grpc.GetConfigVersionPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.meta.MetaMasterConfigurationService", "GetConfigVersion"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetConfigVersionPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetConfigVersionPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new MetaMasterConfigurationServiceMethodDescriptorSupplier("GetConfigVersion"))
                  .build();
          }
        }
     }
     return getGetConfigVersionMethod;
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

    /**
     * <pre>
     **
     * Sets a property for a path.
     * </pre>
     */
    public void setPathConfiguration(alluxio.grpc.SetPathConfigurationPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SetPathConfigurationPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSetPathConfigurationMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Removes properties for a path, if the keys are empty, it means remove all properties.
     * </pre>
     */
    public void removePathConfiguration(alluxio.grpc.RemovePathConfigurationPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RemovePathConfigurationPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRemovePathConfigurationMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the versions of cluster and path level configurations.
     * </pre>
     */
    public void getConfigVersion(alluxio.grpc.GetConfigVersionPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigVersionPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetConfigVersionMethod(), responseObserver);
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
          .addMethod(
            getSetPathConfigurationMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.SetPathConfigurationPRequest,
                alluxio.grpc.SetPathConfigurationPResponse>(
                  this, METHODID_SET_PATH_CONFIGURATION)))
          .addMethod(
            getRemovePathConfigurationMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RemovePathConfigurationPRequest,
                alluxio.grpc.RemovePathConfigurationPResponse>(
                  this, METHODID_REMOVE_PATH_CONFIGURATION)))
          .addMethod(
            getGetConfigVersionMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetConfigVersionPOptions,
                alluxio.grpc.GetConfigVersionPResponse>(
                  this, METHODID_GET_CONFIG_VERSION)))
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

    /**
     * <pre>
     **
     * Sets a property for a path.
     * </pre>
     */
    public void setPathConfiguration(alluxio.grpc.SetPathConfigurationPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SetPathConfigurationPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSetPathConfigurationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Removes properties for a path, if the keys are empty, it means remove all properties.
     * </pre>
     */
    public void removePathConfiguration(alluxio.grpc.RemovePathConfigurationPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RemovePathConfigurationPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRemovePathConfigurationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the versions of cluster and path level configurations.
     * </pre>
     */
    public void getConfigVersion(alluxio.grpc.GetConfigVersionPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigVersionPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetConfigVersionMethod(), getCallOptions()), request, responseObserver);
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

    /**
     * <pre>
     **
     * Sets a property for a path.
     * </pre>
     */
    public alluxio.grpc.SetPathConfigurationPResponse setPathConfiguration(alluxio.grpc.SetPathConfigurationPRequest request) {
      return blockingUnaryCall(
          getChannel(), getSetPathConfigurationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Removes properties for a path, if the keys are empty, it means remove all properties.
     * </pre>
     */
    public alluxio.grpc.RemovePathConfigurationPResponse removePathConfiguration(alluxio.grpc.RemovePathConfigurationPRequest request) {
      return blockingUnaryCall(
          getChannel(), getRemovePathConfigurationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the versions of cluster and path level configurations.
     * </pre>
     */
    public alluxio.grpc.GetConfigVersionPResponse getConfigVersion(alluxio.grpc.GetConfigVersionPOptions request) {
      return blockingUnaryCall(
          getChannel(), getGetConfigVersionMethod(), getCallOptions(), request);
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

    /**
     * <pre>
     **
     * Sets a property for a path.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.SetPathConfigurationPResponse> setPathConfiguration(
        alluxio.grpc.SetPathConfigurationPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSetPathConfigurationMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Removes properties for a path, if the keys are empty, it means remove all properties.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RemovePathConfigurationPResponse> removePathConfiguration(
        alluxio.grpc.RemovePathConfigurationPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRemovePathConfigurationMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the versions of cluster and path level configurations.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetConfigVersionPResponse> getConfigVersion(
        alluxio.grpc.GetConfigVersionPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getGetConfigVersionMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_CONFIGURATION = 0;
  private static final int METHODID_SET_PATH_CONFIGURATION = 1;
  private static final int METHODID_REMOVE_PATH_CONFIGURATION = 2;
  private static final int METHODID_GET_CONFIG_VERSION = 3;

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
        case METHODID_SET_PATH_CONFIGURATION:
          serviceImpl.setPathConfiguration((alluxio.grpc.SetPathConfigurationPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.SetPathConfigurationPResponse>) responseObserver);
          break;
        case METHODID_REMOVE_PATH_CONFIGURATION:
          serviceImpl.removePathConfiguration((alluxio.grpc.RemovePathConfigurationPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RemovePathConfigurationPResponse>) responseObserver);
          break;
        case METHODID_GET_CONFIG_VERSION:
          serviceImpl.getConfigVersion((alluxio.grpc.GetConfigVersionPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigVersionPResponse>) responseObserver);
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
              .addMethod(getSetPathConfigurationMethod())
              .addMethod(getRemovePathConfigurationMethod())
              .addMethod(getGetConfigVersionMethod())
              .build();
        }
      }
    }
    return result;
  }
}
