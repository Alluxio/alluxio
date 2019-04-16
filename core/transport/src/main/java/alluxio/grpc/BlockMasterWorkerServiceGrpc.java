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
 * This interface contains block master service endpoints for Alluxio workers.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: grpc/block_master.proto")
public final class BlockMasterWorkerServiceGrpc {

  private BlockMasterWorkerServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.block.BlockMasterWorkerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.BlockHeartbeatPRequest,
      alluxio.grpc.BlockHeartbeatPResponse> getBlockHeartbeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BlockHeartbeat",
      requestType = alluxio.grpc.BlockHeartbeatPRequest.class,
      responseType = alluxio.grpc.BlockHeartbeatPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.BlockHeartbeatPRequest,
      alluxio.grpc.BlockHeartbeatPResponse> getBlockHeartbeatMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.BlockHeartbeatPRequest, alluxio.grpc.BlockHeartbeatPResponse> getBlockHeartbeatMethod;
    if ((getBlockHeartbeatMethod = BlockMasterWorkerServiceGrpc.getBlockHeartbeatMethod) == null) {
      synchronized (BlockMasterWorkerServiceGrpc.class) {
        if ((getBlockHeartbeatMethod = BlockMasterWorkerServiceGrpc.getBlockHeartbeatMethod) == null) {
          BlockMasterWorkerServiceGrpc.getBlockHeartbeatMethod = getBlockHeartbeatMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.BlockHeartbeatPRequest, alluxio.grpc.BlockHeartbeatPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.block.BlockMasterWorkerService", "BlockHeartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.BlockHeartbeatPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.BlockHeartbeatPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterWorkerServiceMethodDescriptorSupplier("BlockHeartbeat"))
                  .build();
          }
        }
     }
     return getBlockHeartbeatMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CommitBlockPRequest,
      alluxio.grpc.CommitBlockPResponse> getCommitBlockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CommitBlock",
      requestType = alluxio.grpc.CommitBlockPRequest.class,
      responseType = alluxio.grpc.CommitBlockPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CommitBlockPRequest,
      alluxio.grpc.CommitBlockPResponse> getCommitBlockMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CommitBlockPRequest, alluxio.grpc.CommitBlockPResponse> getCommitBlockMethod;
    if ((getCommitBlockMethod = BlockMasterWorkerServiceGrpc.getCommitBlockMethod) == null) {
      synchronized (BlockMasterWorkerServiceGrpc.class) {
        if ((getCommitBlockMethod = BlockMasterWorkerServiceGrpc.getCommitBlockMethod) == null) {
          BlockMasterWorkerServiceGrpc.getCommitBlockMethod = getCommitBlockMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CommitBlockPRequest, alluxio.grpc.CommitBlockPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.block.BlockMasterWorkerService", "CommitBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CommitBlockPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CommitBlockPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterWorkerServiceMethodDescriptorSupplier("CommitBlock"))
                  .build();
          }
        }
     }
     return getCommitBlockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CommitBlockInUfsPRequest,
      alluxio.grpc.CommitBlockInUfsPResponse> getCommitBlockInUfsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CommitBlockInUfs",
      requestType = alluxio.grpc.CommitBlockInUfsPRequest.class,
      responseType = alluxio.grpc.CommitBlockInUfsPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CommitBlockInUfsPRequest,
      alluxio.grpc.CommitBlockInUfsPResponse> getCommitBlockInUfsMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CommitBlockInUfsPRequest, alluxio.grpc.CommitBlockInUfsPResponse> getCommitBlockInUfsMethod;
    if ((getCommitBlockInUfsMethod = BlockMasterWorkerServiceGrpc.getCommitBlockInUfsMethod) == null) {
      synchronized (BlockMasterWorkerServiceGrpc.class) {
        if ((getCommitBlockInUfsMethod = BlockMasterWorkerServiceGrpc.getCommitBlockInUfsMethod) == null) {
          BlockMasterWorkerServiceGrpc.getCommitBlockInUfsMethod = getCommitBlockInUfsMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CommitBlockInUfsPRequest, alluxio.grpc.CommitBlockInUfsPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.block.BlockMasterWorkerService", "CommitBlockInUfs"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CommitBlockInUfsPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CommitBlockInUfsPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterWorkerServiceMethodDescriptorSupplier("CommitBlockInUfs"))
                  .build();
          }
        }
     }
     return getCommitBlockInUfsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerIdPRequest,
      alluxio.grpc.GetWorkerIdPResponse> getGetWorkerIdMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetWorkerId",
      requestType = alluxio.grpc.GetWorkerIdPRequest.class,
      responseType = alluxio.grpc.GetWorkerIdPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerIdPRequest,
      alluxio.grpc.GetWorkerIdPResponse> getGetWorkerIdMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerIdPRequest, alluxio.grpc.GetWorkerIdPResponse> getGetWorkerIdMethod;
    if ((getGetWorkerIdMethod = BlockMasterWorkerServiceGrpc.getGetWorkerIdMethod) == null) {
      synchronized (BlockMasterWorkerServiceGrpc.class) {
        if ((getGetWorkerIdMethod = BlockMasterWorkerServiceGrpc.getGetWorkerIdMethod) == null) {
          BlockMasterWorkerServiceGrpc.getGetWorkerIdMethod = getGetWorkerIdMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetWorkerIdPRequest, alluxio.grpc.GetWorkerIdPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.block.BlockMasterWorkerService", "GetWorkerId"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetWorkerIdPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetWorkerIdPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterWorkerServiceMethodDescriptorSupplier("GetWorkerId"))
                  .build();
          }
        }
     }
     return getGetWorkerIdMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RegisterWorkerPRequest,
      alluxio.grpc.RegisterWorkerPResponse> getRegisterWorkerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterWorker",
      requestType = alluxio.grpc.RegisterWorkerPRequest.class,
      responseType = alluxio.grpc.RegisterWorkerPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.RegisterWorkerPRequest,
      alluxio.grpc.RegisterWorkerPResponse> getRegisterWorkerMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.RegisterWorkerPRequest, alluxio.grpc.RegisterWorkerPResponse> getRegisterWorkerMethod;
    if ((getRegisterWorkerMethod = BlockMasterWorkerServiceGrpc.getRegisterWorkerMethod) == null) {
      synchronized (BlockMasterWorkerServiceGrpc.class) {
        if ((getRegisterWorkerMethod = BlockMasterWorkerServiceGrpc.getRegisterWorkerMethod) == null) {
          BlockMasterWorkerServiceGrpc.getRegisterWorkerMethod = getRegisterWorkerMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.RegisterWorkerPRequest, alluxio.grpc.RegisterWorkerPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.block.BlockMasterWorkerService", "RegisterWorker"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RegisterWorkerPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RegisterWorkerPResponse.getDefaultInstance()))
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
    public void blockHeartbeat(alluxio.grpc.BlockHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.BlockHeartbeatPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getBlockHeartbeatMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed.
     * </pre>
     */
    public void commitBlock(alluxio.grpc.CommitBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CommitBlockPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCommitBlockMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed which resides in UFS.
     * </pre>
     */
    public void commitBlockInUfs(alluxio.grpc.CommitBlockInUfsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CommitBlockInUfsPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCommitBlockInUfsMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a worker id for the given network address.
     * </pre>
     */
    public void getWorkerId(alluxio.grpc.GetWorkerIdPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerIdPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetWorkerIdMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Registers a worker.
     * </pre>
     */
    public void registerWorker(alluxio.grpc.RegisterWorkerPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRegisterWorkerMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getBlockHeartbeatMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.BlockHeartbeatPRequest,
                alluxio.grpc.BlockHeartbeatPResponse>(
                  this, METHODID_BLOCK_HEARTBEAT)))
          .addMethod(
            getCommitBlockMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CommitBlockPRequest,
                alluxio.grpc.CommitBlockPResponse>(
                  this, METHODID_COMMIT_BLOCK)))
          .addMethod(
            getCommitBlockInUfsMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CommitBlockInUfsPRequest,
                alluxio.grpc.CommitBlockInUfsPResponse>(
                  this, METHODID_COMMIT_BLOCK_IN_UFS)))
          .addMethod(
            getGetWorkerIdMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetWorkerIdPRequest,
                alluxio.grpc.GetWorkerIdPResponse>(
                  this, METHODID_GET_WORKER_ID)))
          .addMethod(
            getRegisterWorkerMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RegisterWorkerPRequest,
                alluxio.grpc.RegisterWorkerPResponse>(
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
    public void blockHeartbeat(alluxio.grpc.BlockHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.BlockHeartbeatPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getBlockHeartbeatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed.
     * </pre>
     */
    public void commitBlock(alluxio.grpc.CommitBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CommitBlockPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCommitBlockMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed which resides in UFS.
     * </pre>
     */
    public void commitBlockInUfs(alluxio.grpc.CommitBlockInUfsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CommitBlockInUfsPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCommitBlockInUfsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a worker id for the given network address.
     * </pre>
     */
    public void getWorkerId(alluxio.grpc.GetWorkerIdPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerIdPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetWorkerIdMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Registers a worker.
     * </pre>
     */
    public void registerWorker(alluxio.grpc.RegisterWorkerPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRegisterWorkerMethod(), getCallOptions()), request, responseObserver);
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
    public alluxio.grpc.BlockHeartbeatPResponse blockHeartbeat(alluxio.grpc.BlockHeartbeatPRequest request) {
      return blockingUnaryCall(
          getChannel(), getBlockHeartbeatMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed.
     * </pre>
     */
    public alluxio.grpc.CommitBlockPResponse commitBlock(alluxio.grpc.CommitBlockPRequest request) {
      return blockingUnaryCall(
          getChannel(), getCommitBlockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed which resides in UFS.
     * </pre>
     */
    public alluxio.grpc.CommitBlockInUfsPResponse commitBlockInUfs(alluxio.grpc.CommitBlockInUfsPRequest request) {
      return blockingUnaryCall(
          getChannel(), getCommitBlockInUfsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a worker id for the given network address.
     * </pre>
     */
    public alluxio.grpc.GetWorkerIdPResponse getWorkerId(alluxio.grpc.GetWorkerIdPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetWorkerIdMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Registers a worker.
     * </pre>
     */
    public alluxio.grpc.RegisterWorkerPResponse registerWorker(alluxio.grpc.RegisterWorkerPRequest request) {
      return blockingUnaryCall(
          getChannel(), getRegisterWorkerMethod(), getCallOptions(), request);
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
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.BlockHeartbeatPResponse> blockHeartbeat(
        alluxio.grpc.BlockHeartbeatPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getBlockHeartbeatMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CommitBlockPResponse> commitBlock(
        alluxio.grpc.CommitBlockPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCommitBlockMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Marks the given block as committed which resides in UFS.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CommitBlockInUfsPResponse> commitBlockInUfs(
        alluxio.grpc.CommitBlockInUfsPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCommitBlockInUfsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a worker id for the given network address.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetWorkerIdPResponse> getWorkerId(
        alluxio.grpc.GetWorkerIdPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetWorkerIdMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Registers a worker.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RegisterWorkerPResponse> registerWorker(
        alluxio.grpc.RegisterWorkerPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRegisterWorkerMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_BLOCK_HEARTBEAT = 0;
  private static final int METHODID_COMMIT_BLOCK = 1;
  private static final int METHODID_COMMIT_BLOCK_IN_UFS = 2;
  private static final int METHODID_GET_WORKER_ID = 3;
  private static final int METHODID_REGISTER_WORKER = 4;

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
          serviceImpl.blockHeartbeat((alluxio.grpc.BlockHeartbeatPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.BlockHeartbeatPResponse>) responseObserver);
          break;
        case METHODID_COMMIT_BLOCK:
          serviceImpl.commitBlock((alluxio.grpc.CommitBlockPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CommitBlockPResponse>) responseObserver);
          break;
        case METHODID_COMMIT_BLOCK_IN_UFS:
          serviceImpl.commitBlockInUfs((alluxio.grpc.CommitBlockInUfsPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CommitBlockInUfsPResponse>) responseObserver);
          break;
        case METHODID_GET_WORKER_ID:
          serviceImpl.getWorkerId((alluxio.grpc.GetWorkerIdPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerIdPResponse>) responseObserver);
          break;
        case METHODID_REGISTER_WORKER:
          serviceImpl.registerWorker((alluxio.grpc.RegisterWorkerPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerPResponse>) responseObserver);
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
      return alluxio.grpc.BlockMasterProto.getDescriptor();
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
              .addMethod(getBlockHeartbeatMethod())
              .addMethod(getCommitBlockMethod())
              .addMethod(getCommitBlockInUfsMethod())
              .addMethod(getGetWorkerIdMethod())
              .addMethod(getRegisterWorkerMethod())
              .build();
        }
      }
    }
    return result;
  }
}
