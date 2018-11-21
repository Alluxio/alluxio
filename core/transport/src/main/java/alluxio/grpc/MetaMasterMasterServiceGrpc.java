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
 * This interface contains meta master service endpoints for Alluxio standby masters.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.10.1)",
    comments = "Source: meta_master.proto")
public final class MetaMasterMasterServiceGrpc {

  private MetaMasterMasterServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.MetaMasterMasterService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetMasterIdMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetMasterIdPRequest,
      alluxio.grpc.GetMasterIdPResponse> METHOD_GET_MASTER_ID = getGetMasterIdMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetMasterIdPRequest,
      alluxio.grpc.GetMasterIdPResponse> getGetMasterIdMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetMasterIdPRequest,
      alluxio.grpc.GetMasterIdPResponse> getGetMasterIdMethod() {
    return getGetMasterIdMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetMasterIdPRequest,
      alluxio.grpc.GetMasterIdPResponse> getGetMasterIdMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetMasterIdPRequest, alluxio.grpc.GetMasterIdPResponse> getGetMasterIdMethod;
    if ((getGetMasterIdMethod = MetaMasterMasterServiceGrpc.getGetMasterIdMethod) == null) {
      synchronized (MetaMasterMasterServiceGrpc.class) {
        if ((getGetMasterIdMethod = MetaMasterMasterServiceGrpc.getGetMasterIdMethod) == null) {
          MetaMasterMasterServiceGrpc.getGetMasterIdMethod = getGetMasterIdMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetMasterIdPRequest, alluxio.grpc.GetMasterIdPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.MetaMasterMasterService", "GetMasterId"))
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
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getRegisterMasterMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.RegisterMasterPRequest,
      alluxio.grpc.RegisterMasterPResponse> METHOD_REGISTER_MASTER = getRegisterMasterMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RegisterMasterPRequest,
      alluxio.grpc.RegisterMasterPResponse> getRegisterMasterMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.RegisterMasterPRequest,
      alluxio.grpc.RegisterMasterPResponse> getRegisterMasterMethod() {
    return getRegisterMasterMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.RegisterMasterPRequest,
      alluxio.grpc.RegisterMasterPResponse> getRegisterMasterMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.RegisterMasterPRequest, alluxio.grpc.RegisterMasterPResponse> getRegisterMasterMethod;
    if ((getRegisterMasterMethod = MetaMasterMasterServiceGrpc.getRegisterMasterMethod) == null) {
      synchronized (MetaMasterMasterServiceGrpc.class) {
        if ((getRegisterMasterMethod = MetaMasterMasterServiceGrpc.getRegisterMasterMethod) == null) {
          MetaMasterMasterServiceGrpc.getRegisterMasterMethod = getRegisterMasterMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.RegisterMasterPRequest, alluxio.grpc.RegisterMasterPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.MetaMasterMasterService", "RegisterMaster"))
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
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getMasterHeartbeatMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.MasterHeartbeatPRequest,
      alluxio.grpc.MasterHeartbeatPResponse> METHOD_MASTER_HEARTBEAT = getMasterHeartbeatMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.MasterHeartbeatPRequest,
      alluxio.grpc.MasterHeartbeatPResponse> getMasterHeartbeatMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.MasterHeartbeatPRequest,
      alluxio.grpc.MasterHeartbeatPResponse> getMasterHeartbeatMethod() {
    return getMasterHeartbeatMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.MasterHeartbeatPRequest,
      alluxio.grpc.MasterHeartbeatPResponse> getMasterHeartbeatMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.MasterHeartbeatPRequest, alluxio.grpc.MasterHeartbeatPResponse> getMasterHeartbeatMethod;
    if ((getMasterHeartbeatMethod = MetaMasterMasterServiceGrpc.getMasterHeartbeatMethod) == null) {
      synchronized (MetaMasterMasterServiceGrpc.class) {
        if ((getMasterHeartbeatMethod = MetaMasterMasterServiceGrpc.getMasterHeartbeatMethod) == null) {
          MetaMasterMasterServiceGrpc.getMasterHeartbeatMethod = getMasterHeartbeatMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.MasterHeartbeatPRequest, alluxio.grpc.MasterHeartbeatPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.MetaMasterMasterService", "MasterHeartbeat"))
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
    return new MetaMasterMasterServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetaMasterMasterServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new MetaMasterMasterServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MetaMasterMasterServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new MetaMasterMasterServiceFutureStub(channel);
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
      asyncUnimplementedUnaryCall(getGetMasterIdMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Registers a master.
     * </pre>
     */
    public void registerMaster(alluxio.grpc.RegisterMasterPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RegisterMasterPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRegisterMasterMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Heartbeats to indicate the master is lost or not.
     * </pre>
     */
    public void masterHeartbeat(alluxio.grpc.MasterHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MasterHeartbeatPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getMasterHeartbeatMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetMasterIdMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetMasterIdPRequest,
                alluxio.grpc.GetMasterIdPResponse>(
                  this, METHODID_GET_MASTER_ID)))
          .addMethod(
            getRegisterMasterMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RegisterMasterPRequest,
                alluxio.grpc.RegisterMasterPResponse>(
                  this, METHODID_REGISTER_MASTER)))
          .addMethod(
            getMasterHeartbeatMethodHelper(),
            asyncUnaryCall(
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
  public static final class MetaMasterMasterServiceStub extends io.grpc.stub.AbstractStub<MetaMasterMasterServiceStub> {
    private MetaMasterMasterServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaMasterMasterServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterMasterServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
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
      asyncUnaryCall(
          getChannel().newCall(getGetMasterIdMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Registers a master.
     * </pre>
     */
    public void registerMaster(alluxio.grpc.RegisterMasterPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RegisterMasterPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRegisterMasterMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Heartbeats to indicate the master is lost or not.
     * </pre>
     */
    public void masterHeartbeat(alluxio.grpc.MasterHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MasterHeartbeatPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getMasterHeartbeatMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio standby masters.
   * </pre>
   */
  public static final class MetaMasterMasterServiceBlockingStub extends io.grpc.stub.AbstractStub<MetaMasterMasterServiceBlockingStub> {
    private MetaMasterMasterServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaMasterMasterServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterMasterServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaMasterMasterServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns a master id for the given master address.
     * </pre>
     */
    public alluxio.grpc.GetMasterIdPResponse getMasterId(alluxio.grpc.GetMasterIdPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetMasterIdMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Registers a master.
     * </pre>
     */
    public alluxio.grpc.RegisterMasterPResponse registerMaster(alluxio.grpc.RegisterMasterPRequest request) {
      return blockingUnaryCall(
          getChannel(), getRegisterMasterMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Heartbeats to indicate the master is lost or not.
     * </pre>
     */
    public alluxio.grpc.MasterHeartbeatPResponse masterHeartbeat(alluxio.grpc.MasterHeartbeatPRequest request) {
      return blockingUnaryCall(
          getChannel(), getMasterHeartbeatMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio standby masters.
   * </pre>
   */
  public static final class MetaMasterMasterServiceFutureStub extends io.grpc.stub.AbstractStub<MetaMasterMasterServiceFutureStub> {
    private MetaMasterMasterServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaMasterMasterServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterMasterServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
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
      return futureUnaryCall(
          getChannel().newCall(getGetMasterIdMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Registers a master.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RegisterMasterPResponse> registerMaster(
        alluxio.grpc.RegisterMasterPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRegisterMasterMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Heartbeats to indicate the master is lost or not.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.MasterHeartbeatPResponse> masterHeartbeat(
        alluxio.grpc.MasterHeartbeatPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getMasterHeartbeatMethodHelper(), getCallOptions()), request);
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
              .addMethod(getGetMasterIdMethodHelper())
              .addMethod(getRegisterMasterMethodHelper())
              .addMethod(getMasterHeartbeatMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
