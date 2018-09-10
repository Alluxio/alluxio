package alluxio.grpc.block;

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
 * This interface contains block master service endpoints for Alluxio workers.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.10.1)",
    comments = "Source: block_master.proto")
public final class BlockMasterWorkerServiceGrpc {

  private BlockMasterWorkerServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.BlockMasterWorkerService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getBlockHeartbeatMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.block.BlockHeartbeatPRequest,
      alluxio.grpc.block.BlockHeartbeatPResponse> METHOD_BLOCK_HEARTBEAT = getBlockHeartbeatMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.block.BlockHeartbeatPRequest,
      alluxio.grpc.block.BlockHeartbeatPResponse> getBlockHeartbeatMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.block.BlockHeartbeatPRequest,
      alluxio.grpc.block.BlockHeartbeatPResponse> getBlockHeartbeatMethod() {
    return getBlockHeartbeatMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.block.BlockHeartbeatPRequest,
      alluxio.grpc.block.BlockHeartbeatPResponse> getBlockHeartbeatMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.block.BlockHeartbeatPRequest, alluxio.grpc.block.BlockHeartbeatPResponse> getBlockHeartbeatMethod;
    if ((getBlockHeartbeatMethod = BlockMasterWorkerServiceGrpc.getBlockHeartbeatMethod) == null) {
      synchronized (BlockMasterWorkerServiceGrpc.class) {
        if ((getBlockHeartbeatMethod = BlockMasterWorkerServiceGrpc.getBlockHeartbeatMethod) == null) {
          BlockMasterWorkerServiceGrpc.getBlockHeartbeatMethod = getBlockHeartbeatMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.block.BlockHeartbeatPRequest, alluxio.grpc.block.BlockHeartbeatPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockMasterWorkerService", "BlockHeartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.BlockHeartbeatPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.BlockHeartbeatPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterWorkerServiceMethodDescriptorSupplier("BlockHeartbeat"))
                  .build();
          }
        }
     }
     return getBlockHeartbeatMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getCommitBlockMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.block.CommitBlockPRequest,
      alluxio.grpc.block.CommitBlockPResponse> METHOD_COMMIT_BLOCK = getCommitBlockMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.block.CommitBlockPRequest,
      alluxio.grpc.block.CommitBlockPResponse> getCommitBlockMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.block.CommitBlockPRequest,
      alluxio.grpc.block.CommitBlockPResponse> getCommitBlockMethod() {
    return getCommitBlockMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.block.CommitBlockPRequest,
      alluxio.grpc.block.CommitBlockPResponse> getCommitBlockMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.block.CommitBlockPRequest, alluxio.grpc.block.CommitBlockPResponse> getCommitBlockMethod;
    if ((getCommitBlockMethod = BlockMasterWorkerServiceGrpc.getCommitBlockMethod) == null) {
      synchronized (BlockMasterWorkerServiceGrpc.class) {
        if ((getCommitBlockMethod = BlockMasterWorkerServiceGrpc.getCommitBlockMethod) == null) {
          BlockMasterWorkerServiceGrpc.getCommitBlockMethod = getCommitBlockMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.block.CommitBlockPRequest, alluxio.grpc.block.CommitBlockPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockMasterWorkerService", "CommitBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.CommitBlockPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.CommitBlockPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterWorkerServiceMethodDescriptorSupplier("CommitBlock"))
                  .build();
          }
        }
     }
     return getCommitBlockMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetWorkerIdMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerIdPRequest,
      alluxio.grpc.block.GetWorkerIdPResponse> METHOD_GET_WORKER_ID = getGetWorkerIdMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerIdPRequest,
      alluxio.grpc.block.GetWorkerIdPResponse> getGetWorkerIdMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerIdPRequest,
      alluxio.grpc.block.GetWorkerIdPResponse> getGetWorkerIdMethod() {
    return getGetWorkerIdMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerIdPRequest,
      alluxio.grpc.block.GetWorkerIdPResponse> getGetWorkerIdMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerIdPRequest, alluxio.grpc.block.GetWorkerIdPResponse> getGetWorkerIdMethod;
    if ((getGetWorkerIdMethod = BlockMasterWorkerServiceGrpc.getGetWorkerIdMethod) == null) {
      synchronized (BlockMasterWorkerServiceGrpc.class) {
        if ((getGetWorkerIdMethod = BlockMasterWorkerServiceGrpc.getGetWorkerIdMethod) == null) {
          BlockMasterWorkerServiceGrpc.getGetWorkerIdMethod = getGetWorkerIdMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.block.GetWorkerIdPRequest, alluxio.grpc.block.GetWorkerIdPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockMasterWorkerService", "GetWorkerId"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetWorkerIdPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetWorkerIdPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterWorkerServiceMethodDescriptorSupplier("GetWorkerId"))
                  .build();
          }
        }
     }
     return getGetWorkerIdMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getRegisterWorkerMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.block.RegisterWorkerPRequest,
      alluxio.grpc.block.RegisterWorkerPResponse> METHOD_REGISTER_WORKER = getRegisterWorkerMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.block.RegisterWorkerPRequest,
      alluxio.grpc.block.RegisterWorkerPResponse> getRegisterWorkerMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.block.RegisterWorkerPRequest,
      alluxio.grpc.block.RegisterWorkerPResponse> getRegisterWorkerMethod() {
    return getRegisterWorkerMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.block.RegisterWorkerPRequest,
      alluxio.grpc.block.RegisterWorkerPResponse> getRegisterWorkerMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.block.RegisterWorkerPRequest, alluxio.grpc.block.RegisterWorkerPResponse> getRegisterWorkerMethod;
    if ((getRegisterWorkerMethod = BlockMasterWorkerServiceGrpc.getRegisterWorkerMethod) == null) {
      synchronized (BlockMasterWorkerServiceGrpc.class) {
        if ((getRegisterWorkerMethod = BlockMasterWorkerServiceGrpc.getRegisterWorkerMethod) == null) {
          BlockMasterWorkerServiceGrpc.getRegisterWorkerMethod = getRegisterWorkerMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.block.RegisterWorkerPRequest, alluxio.grpc.block.RegisterWorkerPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockMasterWorkerService", "RegisterWorker"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.RegisterWorkerPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.RegisterWorkerPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterWorkerServiceMethodDescriptorSupplier("RegisterWorker"))
                  .build();
          }
        }
     }
     return getRegisterWorkerMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BlockMasterWorkerServiceStub newStub(io.grpc.Channel channel) {
    return new BlockMasterWorkerServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BlockMasterWorkerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BlockMasterWorkerServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BlockMasterWorkerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BlockMasterWorkerServiceFutureStub(channel);
  }

  /**
   * <pre>
   **
   * This interface contains block master service endpoints for Alluxio workers.
   * </pre>
   */
  public static abstract class BlockMasterWorkerServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Periodic block worker heartbeat returns an optional command for the block worker to execute.
     * </pre>
     */
    public void blockHeartbeat(alluxio.grpc.block.BlockHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.BlockHeartbeatPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getBlockHeartbeatMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed.
     * </pre>
     */
    public void commitBlock(alluxio.grpc.block.CommitBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.CommitBlockPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCommitBlockMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a worker id for the given network address.
     * </pre>
     */
    public void getWorkerId(alluxio.grpc.block.GetWorkerIdPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetWorkerIdPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetWorkerIdMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Registers a worker.
     * </pre>
     */
    public void registerWorker(alluxio.grpc.block.RegisterWorkerPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.RegisterWorkerPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRegisterWorkerMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getBlockHeartbeatMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.block.BlockHeartbeatPRequest,
                alluxio.grpc.block.BlockHeartbeatPResponse>(
                  this, METHODID_BLOCK_HEARTBEAT)))
          .addMethod(
            getCommitBlockMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.block.CommitBlockPRequest,
                alluxio.grpc.block.CommitBlockPResponse>(
                  this, METHODID_COMMIT_BLOCK)))
          .addMethod(
            getGetWorkerIdMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.block.GetWorkerIdPRequest,
                alluxio.grpc.block.GetWorkerIdPResponse>(
                  this, METHODID_GET_WORKER_ID)))
          .addMethod(
            getRegisterWorkerMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.block.RegisterWorkerPRequest,
                alluxio.grpc.block.RegisterWorkerPResponse>(
                  this, METHODID_REGISTER_WORKER)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains block master service endpoints for Alluxio workers.
   * </pre>
   */
  public static final class BlockMasterWorkerServiceStub extends io.grpc.stub.AbstractStub<BlockMasterWorkerServiceStub> {
    private BlockMasterWorkerServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockMasterWorkerServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockMasterWorkerServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockMasterWorkerServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Periodic block worker heartbeat returns an optional command for the block worker to execute.
     * </pre>
     */
    public void blockHeartbeat(alluxio.grpc.block.BlockHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.BlockHeartbeatPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getBlockHeartbeatMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed.
     * </pre>
     */
    public void commitBlock(alluxio.grpc.block.CommitBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.CommitBlockPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCommitBlockMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a worker id for the given network address.
     * </pre>
     */
    public void getWorkerId(alluxio.grpc.block.GetWorkerIdPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetWorkerIdPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetWorkerIdMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Registers a worker.
     * </pre>
     */
    public void registerWorker(alluxio.grpc.block.RegisterWorkerPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.RegisterWorkerPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRegisterWorkerMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains block master service endpoints for Alluxio workers.
   * </pre>
   */
  public static final class BlockMasterWorkerServiceBlockingStub extends io.grpc.stub.AbstractStub<BlockMasterWorkerServiceBlockingStub> {
    private BlockMasterWorkerServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockMasterWorkerServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockMasterWorkerServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockMasterWorkerServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Periodic block worker heartbeat returns an optional command for the block worker to execute.
     * </pre>
     */
    public alluxio.grpc.block.BlockHeartbeatPResponse blockHeartbeat(alluxio.grpc.block.BlockHeartbeatPRequest request) {
      return blockingUnaryCall(
          getChannel(), getBlockHeartbeatMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed.
     * </pre>
     */
    public alluxio.grpc.block.CommitBlockPResponse commitBlock(alluxio.grpc.block.CommitBlockPRequest request) {
      return blockingUnaryCall(
          getChannel(), getCommitBlockMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a worker id for the given network address.
     * </pre>
     */
    public alluxio.grpc.block.GetWorkerIdPResponse getWorkerId(alluxio.grpc.block.GetWorkerIdPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetWorkerIdMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Registers a worker.
     * </pre>
     */
    public alluxio.grpc.block.RegisterWorkerPResponse registerWorker(alluxio.grpc.block.RegisterWorkerPRequest request) {
      return blockingUnaryCall(
          getChannel(), getRegisterWorkerMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains block master service endpoints for Alluxio workers.
   * </pre>
   */
  public static final class BlockMasterWorkerServiceFutureStub extends io.grpc.stub.AbstractStub<BlockMasterWorkerServiceFutureStub> {
    private BlockMasterWorkerServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockMasterWorkerServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockMasterWorkerServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockMasterWorkerServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Periodic block worker heartbeat returns an optional command for the block worker to execute.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.block.BlockHeartbeatPResponse> blockHeartbeat(
        alluxio.grpc.block.BlockHeartbeatPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getBlockHeartbeatMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.block.CommitBlockPResponse> commitBlock(
        alluxio.grpc.block.CommitBlockPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCommitBlockMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a worker id for the given network address.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.block.GetWorkerIdPResponse> getWorkerId(
        alluxio.grpc.block.GetWorkerIdPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetWorkerIdMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Registers a worker.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.block.RegisterWorkerPResponse> registerWorker(
        alluxio.grpc.block.RegisterWorkerPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRegisterWorkerMethodHelper(), getCallOptions()), request);
    }
  }

  private static final int METHODID_BLOCK_HEARTBEAT = 0;
  private static final int METHODID_COMMIT_BLOCK = 1;
  private static final int METHODID_GET_WORKER_ID = 2;
  private static final int METHODID_REGISTER_WORKER = 3;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BlockMasterWorkerServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BlockMasterWorkerServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_BLOCK_HEARTBEAT:
          serviceImpl.blockHeartbeat((alluxio.grpc.block.BlockHeartbeatPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.block.BlockHeartbeatPResponse>) responseObserver);
          break;
        case METHODID_COMMIT_BLOCK:
          serviceImpl.commitBlock((alluxio.grpc.block.CommitBlockPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.block.CommitBlockPResponse>) responseObserver);
          break;
        case METHODID_GET_WORKER_ID:
          serviceImpl.getWorkerId((alluxio.grpc.block.GetWorkerIdPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.block.GetWorkerIdPResponse>) responseObserver);
          break;
        case METHODID_REGISTER_WORKER:
          serviceImpl.registerWorker((alluxio.grpc.block.RegisterWorkerPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.block.RegisterWorkerPResponse>) responseObserver);
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

  private static abstract class BlockMasterWorkerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BlockMasterWorkerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.block.BlockMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BlockMasterWorkerService");
    }
  }

  private static final class BlockMasterWorkerServiceFileDescriptorSupplier
      extends BlockMasterWorkerServiceBaseDescriptorSupplier {
    BlockMasterWorkerServiceFileDescriptorSupplier() {}
  }

  private static final class BlockMasterWorkerServiceMethodDescriptorSupplier
      extends BlockMasterWorkerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BlockMasterWorkerServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (BlockMasterWorkerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BlockMasterWorkerServiceFileDescriptorSupplier())
              .addMethod(getBlockHeartbeatMethodHelper())
              .addMethod(getCommitBlockMethodHelper())
              .addMethod(getGetWorkerIdMethodHelper())
              .addMethod(getRegisterWorkerMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
