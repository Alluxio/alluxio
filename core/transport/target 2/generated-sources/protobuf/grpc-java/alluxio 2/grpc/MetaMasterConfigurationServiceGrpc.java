package alluxio.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 **
 * This interface contains meta master service endpoints for Alluxio clients
 * to query cluster configuration.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.37.0)",
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
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetConfiguration"))
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
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetPathConfiguration"))
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
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RemovePathConfiguration"))
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

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetConfigHashPOptions,
      alluxio.grpc.GetConfigHashPResponse> getGetConfigHashMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetConfigHash",
      requestType = alluxio.grpc.GetConfigHashPOptions.class,
      responseType = alluxio.grpc.GetConfigHashPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetConfigHashPOptions,
      alluxio.grpc.GetConfigHashPResponse> getGetConfigHashMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetConfigHashPOptions, alluxio.grpc.GetConfigHashPResponse> getGetConfigHashMethod;
    if ((getGetConfigHashMethod = MetaMasterConfigurationServiceGrpc.getGetConfigHashMethod) == null) {
      synchronized (MetaMasterConfigurationServiceGrpc.class) {
        if ((getGetConfigHashMethod = MetaMasterConfigurationServiceGrpc.getGetConfigHashMethod) == null) {
          MetaMasterConfigurationServiceGrpc.getGetConfigHashMethod = getGetConfigHashMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetConfigHashPOptions, alluxio.grpc.GetConfigHashPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetConfigHash"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetConfigHashPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetConfigHashPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetaMasterConfigurationServiceMethodDescriptorSupplier("GetConfigHash"))
              .build();
        }
      }
    }
    return getGetConfigHashMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.UpdateConfigurationPRequest,
      alluxio.grpc.UpdateConfigurationPResponse> getUpdateConfigurationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateConfiguration",
      requestType = alluxio.grpc.UpdateConfigurationPRequest.class,
      responseType = alluxio.grpc.UpdateConfigurationPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.UpdateConfigurationPRequest,
      alluxio.grpc.UpdateConfigurationPResponse> getUpdateConfigurationMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.UpdateConfigurationPRequest, alluxio.grpc.UpdateConfigurationPResponse> getUpdateConfigurationMethod;
    if ((getUpdateConfigurationMethod = MetaMasterConfigurationServiceGrpc.getUpdateConfigurationMethod) == null) {
      synchronized (MetaMasterConfigurationServiceGrpc.class) {
        if ((getUpdateConfigurationMethod = MetaMasterConfigurationServiceGrpc.getUpdateConfigurationMethod) == null) {
          MetaMasterConfigurationServiceGrpc.getUpdateConfigurationMethod = getUpdateConfigurationMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.UpdateConfigurationPRequest, alluxio.grpc.UpdateConfigurationPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateConfiguration"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UpdateConfigurationPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UpdateConfigurationPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetaMasterConfigurationServiceMethodDescriptorSupplier("UpdateConfiguration"))
              .build();
        }
      }
    }
    return getUpdateConfigurationMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MetaMasterConfigurationServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetaMasterConfigurationServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetaMasterConfigurationServiceStub>() {
        @java.lang.Override
        public MetaMasterConfigurationServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetaMasterConfigurationServiceStub(channel, callOptions);
        }
      };
    return MetaMasterConfigurationServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetaMasterConfigurationServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetaMasterConfigurationServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetaMasterConfigurationServiceBlockingStub>() {
        @java.lang.Override
        public MetaMasterConfigurationServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetaMasterConfigurationServiceBlockingStub(channel, callOptions);
        }
      };
    return MetaMasterConfigurationServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MetaMasterConfigurationServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetaMasterConfigurationServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetaMasterConfigurationServiceFutureStub>() {
        @java.lang.Override
        public MetaMasterConfigurationServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetaMasterConfigurationServiceFutureStub(channel, callOptions);
        }
      };
    return MetaMasterConfigurationServiceFutureStub.newStub(factory, channel);
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
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetConfigurationMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Sets a property for a path.
     * </pre>
     */
    public void setPathConfiguration(alluxio.grpc.SetPathConfigurationPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SetPathConfigurationPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetPathConfigurationMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Removes properties for a path, if the keys are empty, it means remove all properties.
     * </pre>
     */
    public void removePathConfiguration(alluxio.grpc.RemovePathConfigurationPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RemovePathConfigurationPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRemovePathConfigurationMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the hashes of cluster and path level configurations.
     * </pre>
     */
    public void getConfigHash(alluxio.grpc.GetConfigHashPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigHashPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetConfigHashMethod(), responseObserver);
    }

    /**
     */
    public void updateConfiguration(alluxio.grpc.UpdateConfigurationPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UpdateConfigurationPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateConfigurationMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetConfigurationMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetConfigurationPOptions,
                alluxio.grpc.GetConfigurationPResponse>(
                  this, METHODID_GET_CONFIGURATION)))
          .addMethod(
            getSetPathConfigurationMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.SetPathConfigurationPRequest,
                alluxio.grpc.SetPathConfigurationPResponse>(
                  this, METHODID_SET_PATH_CONFIGURATION)))
          .addMethod(
            getRemovePathConfigurationMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RemovePathConfigurationPRequest,
                alluxio.grpc.RemovePathConfigurationPResponse>(
                  this, METHODID_REMOVE_PATH_CONFIGURATION)))
          .addMethod(
            getGetConfigHashMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetConfigHashPOptions,
                alluxio.grpc.GetConfigHashPResponse>(
                  this, METHODID_GET_CONFIG_HASH)))
          .addMethod(
            getUpdateConfigurationMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.UpdateConfigurationPRequest,
                alluxio.grpc.UpdateConfigurationPResponse>(
                  this, METHODID_UPDATE_CONFIGURATION)))
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
  public static final class MetaMasterConfigurationServiceStub extends io.grpc.stub.AbstractAsyncStub<MetaMasterConfigurationServiceStub> {
    private MetaMasterConfigurationServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterConfigurationServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
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
      io.grpc.stub.ClientCalls.asyncUnaryCall(
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
      io.grpc.stub.ClientCalls.asyncUnaryCall(
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
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRemovePathConfigurationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the hashes of cluster and path level configurations.
     * </pre>
     */
    public void getConfigHash(alluxio.grpc.GetConfigHashPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigHashPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetConfigHashMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateConfiguration(alluxio.grpc.UpdateConfigurationPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UpdateConfigurationPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateConfigurationMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients
   * to query cluster configuration.
   * </pre>
   */
  public static final class MetaMasterConfigurationServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<MetaMasterConfigurationServiceBlockingStub> {
    private MetaMasterConfigurationServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterConfigurationServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetaMasterConfigurationServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns a list of Alluxio runtime configuration information.
     * </pre>
     */
    public alluxio.grpc.GetConfigurationPResponse getConfiguration(alluxio.grpc.GetConfigurationPOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetConfigurationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Sets a property for a path.
     * </pre>
     */
    public alluxio.grpc.SetPathConfigurationPResponse setPathConfiguration(alluxio.grpc.SetPathConfigurationPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetPathConfigurationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Removes properties for a path, if the keys are empty, it means remove all properties.
     * </pre>
     */
    public alluxio.grpc.RemovePathConfigurationPResponse removePathConfiguration(alluxio.grpc.RemovePathConfigurationPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRemovePathConfigurationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the hashes of cluster and path level configurations.
     * </pre>
     */
    public alluxio.grpc.GetConfigHashPResponse getConfigHash(alluxio.grpc.GetConfigHashPOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetConfigHashMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.UpdateConfigurationPResponse updateConfiguration(alluxio.grpc.UpdateConfigurationPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateConfigurationMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients
   * to query cluster configuration.
   * </pre>
   */
  public static final class MetaMasterConfigurationServiceFutureStub extends io.grpc.stub.AbstractFutureStub<MetaMasterConfigurationServiceFutureStub> {
    private MetaMasterConfigurationServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterConfigurationServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
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
      return io.grpc.stub.ClientCalls.futureUnaryCall(
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
      return io.grpc.stub.ClientCalls.futureUnaryCall(
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
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRemovePathConfigurationMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the hashes of cluster and path level configurations.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetConfigHashPResponse> getConfigHash(
        alluxio.grpc.GetConfigHashPOptions request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetConfigHashMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.UpdateConfigurationPResponse> updateConfiguration(
        alluxio.grpc.UpdateConfigurationPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateConfigurationMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_CONFIGURATION = 0;
  private static final int METHODID_SET_PATH_CONFIGURATION = 1;
  private static final int METHODID_REMOVE_PATH_CONFIGURATION = 2;
  private static final int METHODID_GET_CONFIG_HASH = 3;
  private static final int METHODID_UPDATE_CONFIGURATION = 4;

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
        case METHODID_GET_CONFIG_HASH:
          serviceImpl.getConfigHash((alluxio.grpc.GetConfigHashPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigHashPResponse>) responseObserver);
          break;
        case METHODID_UPDATE_CONFIGURATION:
          serviceImpl.updateConfiguration((alluxio.grpc.UpdateConfigurationPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.UpdateConfigurationPResponse>) responseObserver);
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
              .addMethod(getGetConfigHashMethod())
              .addMethod(getUpdateConfigurationMethod())
              .build();
        }
      }
    }
    return result;
  }
}
