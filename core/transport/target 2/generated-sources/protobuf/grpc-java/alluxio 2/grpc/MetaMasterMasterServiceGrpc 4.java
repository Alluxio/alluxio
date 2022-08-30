package alluxio.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 **
 * This interface contains meta master service endpoints for Alluxio standby masters.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.37.0)",
    comments = "Source: grpc/meta_master.proto")
public final class MetaMasterMasterServiceGrpc {

  private MetaMasterMasterServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.meta.MetaMasterMasterService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetMasterIdPRequest,
      alluxio.grpc.GetMasterIdPResponse> getGetMasterIdMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetMasterId",
      requestType = alluxio.grpc.GetMasterIdPRequest.class,
      responseType = alluxio.grpc.GetMasterIdPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetMasterIdPRequest,
      alluxio.grpc.GetMasterIdPResponse> getGetMasterIdMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetMasterIdPRequest, alluxio.grpc.GetMasterIdPResponse> getGetMasterIdMethod;
    if ((getGetMasterIdMethod = MetaMasterMasterServiceGrpc.getGetMasterIdMethod) == null) {
      synchronized (MetaMasterMasterServiceGrpc.class) {
        if ((getGetMasterIdMethod = MetaMasterMasterServiceGrpc.getGetMasterIdMethod) == null) {
          MetaMasterMasterServiceGrpc.getGetMasterIdMethod = getGetMasterIdMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetMasterIdPRequest, alluxio.grpc.GetMasterIdPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetMasterId"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetMasterIdPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetMasterIdPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetaMasterMasterServiceMethodDescriptorSupplier("GetMasterId"))
              .build();
        }
      }
    }
    return getGetMasterIdMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RegisterMasterPRequest,
      alluxio.grpc.RegisterMasterPResponse> getRegisterMasterMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterMaster",
      requestType = alluxio.grpc.RegisterMasterPRequest.class,
      responseType = alluxio.grpc.RegisterMasterPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.RegisterMasterPRequest,
      alluxio.grpc.RegisterMasterPResponse> getRegisterMasterMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.RegisterMasterPRequest, alluxio.grpc.RegisterMasterPResponse> getRegisterMasterMethod;
    if ((getRegisterMasterMethod = MetaMasterMasterServiceGrpc.getRegisterMasterMethod) == null) {
      synchronized (MetaMasterMasterServiceGrpc.class) {
        if ((getRegisterMasterMethod = MetaMasterMasterServiceGrpc.getRegisterMasterMethod) == null) {
          MetaMasterMasterServiceGrpc.getRegisterMasterMethod = getRegisterMasterMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.RegisterMasterPRequest, alluxio.grpc.RegisterMasterPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RegisterMaster"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RegisterMasterPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RegisterMasterPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetaMasterMasterServiceMethodDescriptorSupplier("RegisterMaster"))
              .build();
        }
      }
    }
    return getRegisterMasterMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.MasterHeartbeatPRequest,
      alluxio.grpc.MasterHeartbeatPResponse> getMasterHeartbeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MasterHeartbeat",
      requestType = alluxio.grpc.MasterHeartbeatPRequest.class,
      responseType = alluxio.grpc.MasterHeartbeatPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.MasterHeartbeatPRequest,
      alluxio.grpc.MasterHeartbeatPResponse> getMasterHeartbeatMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.MasterHeartbeatPRequest, alluxio.grpc.MasterHeartbeatPResponse> getMasterHeartbeatMethod;
    if ((getMasterHeartbeatMethod = MetaMasterMasterServiceGrpc.getMasterHeartbeatMethod) == null) {
      synchronized (MetaMasterMasterServiceGrpc.class) {
        if ((getMasterHeartbeatMethod = MetaMasterMasterServiceGrpc.getMasterHeartbeatMethod) == null) {
          MetaMasterMasterServiceGrpc.getMasterHeartbeatMethod = getMasterHeartbeatMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.MasterHeartbeatPRequest, alluxio.grpc.MasterHeartbeatPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MasterHeartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.MasterHeartbeatPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.MasterHeartbeatPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetaMasterMasterServiceMethodDescriptorSupplier("MasterHeartbeat"))
              .build();
        }
      }
    }
    return getMasterHeartbeatMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MetaMasterMasterServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetaMasterMasterServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetaMasterMasterServiceStub>() {
        @java.lang.Override
        public MetaMasterMasterServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetaMasterMasterServiceStub(channel, callOptions);
        }
      };
    return MetaMasterMasterServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetaMasterMasterServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetaMasterMasterServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetaMasterMasterServiceBlockingStub>() {
        @java.lang.Override
        public MetaMasterMasterServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetaMasterMasterServiceBlockingStub(channel, callOptions);
        }
      };
    return MetaMasterMasterServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MetaMasterMasterServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetaMasterMasterServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetaMasterMasterServiceFutureStub>() {
        @java.lang.Override
        public MetaMasterMasterServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetaMasterMasterServiceFutureStub(channel, callOptions);
        }
      };
    return MetaMasterMasterServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio standby masters.
   * </pre>
   */
  public static abstract class MetaMasterMasterServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Returns a master id for the given master address.
     * </pre>
     */
    public void getMasterId(alluxio.grpc.GetMasterIdPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMasterIdPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMasterIdMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Registers a master.
     * </pre>
     */
    public void registerMaster(alluxio.grpc.RegisterMasterPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RegisterMasterPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRegisterMasterMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Heartbeats to indicate the master is lost or not.
     * </pre>
     */
    public void masterHeartbeat(alluxio.grpc.MasterHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MasterHeartbeatPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMasterHeartbeatMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetMasterIdMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetMasterIdPRequest,
                alluxio.grpc.GetMasterIdPResponse>(
                  this, METHODID_GET_MASTER_ID)))
          .addMethod(
            getRegisterMasterMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RegisterMasterPRequest,
                alluxio.grpc.RegisterMasterPResponse>(
                  this, METHODID_REGISTER_MASTER)))
          .addMethod(
            getMasterHeartbeatMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.MasterHeartbeatPRequest,
                alluxio.grpc.MasterHeartbeatPResponse>(
                  this, METHODID_MASTER_HEARTBEAT)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio standby masters.
   * </pre>
   */
  public static final class MetaMasterMasterServiceStub extends io.grpc.stub.AbstractAsyncStub<MetaMasterMasterServiceStub> {
    private MetaMasterMasterServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterMasterServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetaMasterMasterServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns a master id for the given master address.
     * </pre>
     */
    public void getMasterId(alluxio.grpc.GetMasterIdPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMasterIdPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMasterIdMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Registers a master.
     * </pre>
     */
    public void registerMaster(alluxio.grpc.RegisterMasterPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RegisterMasterPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRegisterMasterMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Heartbeats to indicate the master is lost or not.
     * </pre>
     */
    public void masterHeartbeat(alluxio.grpc.MasterHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MasterHeartbeatPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMasterHeartbeatMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio standby masters.
   * </pre>
   */
  public static final class MetaMasterMasterServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<MetaMasterMasterServiceBlockingStub> {
    private MetaMasterMasterServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterMasterServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetaMasterMasterServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns a master id for the given master address.
     * </pre>
     */
    public alluxio.grpc.GetMasterIdPResponse getMasterId(alluxio.grpc.GetMasterIdPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMasterIdMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Registers a master.
     * </pre>
     */
    public alluxio.grpc.RegisterMasterPResponse registerMaster(alluxio.grpc.RegisterMasterPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRegisterMasterMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Heartbeats to indicate the master is lost or not.
     * </pre>
     */
    public alluxio.grpc.MasterHeartbeatPResponse masterHeartbeat(alluxio.grpc.MasterHeartbeatPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMasterHeartbeatMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio standby masters.
   * </pre>
   */
  public static final class MetaMasterMasterServiceFutureStub extends io.grpc.stub.AbstractFutureStub<MetaMasterMasterServiceFutureStub> {
    private MetaMasterMasterServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterMasterServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetaMasterMasterServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns a master id for the given master address.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetMasterIdPResponse> getMasterId(
        alluxio.grpc.GetMasterIdPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMasterIdMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Registers a master.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RegisterMasterPResponse> registerMaster(
        alluxio.grpc.RegisterMasterPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRegisterMasterMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Heartbeats to indicate the master is lost or not.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.MasterHeartbeatPResponse> masterHeartbeat(
        alluxio.grpc.MasterHeartbeatPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMasterHeartbeatMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_MASTER_ID = 0;
  private static final int METHODID_REGISTER_MASTER = 1;
  private static final int METHODID_MASTER_HEARTBEAT = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MetaMasterMasterServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(MetaMasterMasterServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_MASTER_ID:
          serviceImpl.getMasterId((alluxio.grpc.GetMasterIdPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetMasterIdPResponse>) responseObserver);
          break;
        case METHODID_REGISTER_MASTER:
          serviceImpl.registerMaster((alluxio.grpc.RegisterMasterPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RegisterMasterPResponse>) responseObserver);
          break;
        case METHODID_MASTER_HEARTBEAT:
          serviceImpl.masterHeartbeat((alluxio.grpc.MasterHeartbeatPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.MasterHeartbeatPResponse>) responseObserver);
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

  private static abstract class MetaMasterMasterServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MetaMasterMasterServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.MetaMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("MetaMasterMasterService");
    }
  }

  private static final class MetaMasterMasterServiceFileDescriptorSupplier
      extends MetaMasterMasterServiceBaseDescriptorSupplier {
    MetaMasterMasterServiceFileDescriptorSupplier() {}
  }

  private static final class MetaMasterMasterServiceMethodDescriptorSupplier
      extends MetaMasterMasterServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    MetaMasterMasterServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (MetaMasterMasterServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MetaMasterMasterServiceFileDescriptorSupplier())
              .addMethod(getGetMasterIdMethod())
              .addMethod(getRegisterMasterMethod())
              .addMethod(getMasterHeartbeatMethod())
              .build();
        }
      }
    }
    return result;
  }
}
