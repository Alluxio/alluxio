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
    comments = "Source: block_worker.proto")
public final class BlockWorkerClientServiceGrpc {

  private BlockWorkerClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.BlockWorkerClientService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getAccessBlockMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.AccessBlockPRequest,
      alluxio.grpc.AccessBlockPResponse> METHOD_ACCESS_BLOCK = getAccessBlockMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.AccessBlockPRequest,
      alluxio.grpc.AccessBlockPResponse> getAccessBlockMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.AccessBlockPRequest,
      alluxio.grpc.AccessBlockPResponse> getAccessBlockMethod() {
    return getAccessBlockMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.AccessBlockPRequest,
      alluxio.grpc.AccessBlockPResponse> getAccessBlockMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.AccessBlockPRequest, alluxio.grpc.AccessBlockPResponse> getAccessBlockMethod;
    if ((getAccessBlockMethod = BlockWorkerClientServiceGrpc.getAccessBlockMethod) == null) {
      synchronized (BlockWorkerClientServiceGrpc.class) {
        if ((getAccessBlockMethod = BlockWorkerClientServiceGrpc.getAccessBlockMethod) == null) {
          BlockWorkerClientServiceGrpc.getAccessBlockMethod = getAccessBlockMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.AccessBlockPRequest, alluxio.grpc.AccessBlockPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorkerClientService", "accessBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.AccessBlockPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.AccessBlockPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerClientServiceMethodDescriptorSupplier("accessBlock"))
                  .build();
          }
        }
     }
     return getAccessBlockMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getCacheBlockMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.CacheBlockPRequest,
      alluxio.grpc.CacheBlockPResponse> METHOD_CACHE_BLOCK = getCacheBlockMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CacheBlockPRequest,
      alluxio.grpc.CacheBlockPResponse> getCacheBlockMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.CacheBlockPRequest,
      alluxio.grpc.CacheBlockPResponse> getCacheBlockMethod() {
    return getCacheBlockMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.CacheBlockPRequest,
      alluxio.grpc.CacheBlockPResponse> getCacheBlockMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.CacheBlockPRequest, alluxio.grpc.CacheBlockPResponse> getCacheBlockMethod;
    if ((getCacheBlockMethod = BlockWorkerClientServiceGrpc.getCacheBlockMethod) == null) {
      synchronized (BlockWorkerClientServiceGrpc.class) {
        if ((getCacheBlockMethod = BlockWorkerClientServiceGrpc.getCacheBlockMethod) == null) {
          BlockWorkerClientServiceGrpc.getCacheBlockMethod = getCacheBlockMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CacheBlockPRequest, alluxio.grpc.CacheBlockPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorkerClientService", "cacheBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CacheBlockPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CacheBlockPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerClientServiceMethodDescriptorSupplier("cacheBlock"))
                  .build();
          }
        }
     }
     return getCacheBlockMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getCancelBlockMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.CancelBlockPRequest,
      alluxio.grpc.CancelBlockPResponse> METHOD_CANCEL_BLOCK = getCancelBlockMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CancelBlockPRequest,
      alluxio.grpc.CancelBlockPResponse> getCancelBlockMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.CancelBlockPRequest,
      alluxio.grpc.CancelBlockPResponse> getCancelBlockMethod() {
    return getCancelBlockMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.CancelBlockPRequest,
      alluxio.grpc.CancelBlockPResponse> getCancelBlockMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.CancelBlockPRequest, alluxio.grpc.CancelBlockPResponse> getCancelBlockMethod;
    if ((getCancelBlockMethod = BlockWorkerClientServiceGrpc.getCancelBlockMethod) == null) {
      synchronized (BlockWorkerClientServiceGrpc.class) {
        if ((getCancelBlockMethod = BlockWorkerClientServiceGrpc.getCancelBlockMethod) == null) {
          BlockWorkerClientServiceGrpc.getCancelBlockMethod = getCancelBlockMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CancelBlockPRequest, alluxio.grpc.CancelBlockPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorkerClientService", "CancelBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CancelBlockPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CancelBlockPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerClientServiceMethodDescriptorSupplier("CancelBlock"))
                  .build();
          }
        }
     }
     return getCancelBlockMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getLockBlockMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.LockBlockPRequest,
      alluxio.grpc.LockBlockPResponse> METHOD_LOCK_BLOCK = getLockBlockMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.LockBlockPRequest,
      alluxio.grpc.LockBlockPResponse> getLockBlockMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.LockBlockPRequest,
      alluxio.grpc.LockBlockPResponse> getLockBlockMethod() {
    return getLockBlockMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.LockBlockPRequest,
      alluxio.grpc.LockBlockPResponse> getLockBlockMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.LockBlockPRequest, alluxio.grpc.LockBlockPResponse> getLockBlockMethod;
    if ((getLockBlockMethod = BlockWorkerClientServiceGrpc.getLockBlockMethod) == null) {
      synchronized (BlockWorkerClientServiceGrpc.class) {
        if ((getLockBlockMethod = BlockWorkerClientServiceGrpc.getLockBlockMethod) == null) {
          BlockWorkerClientServiceGrpc.getLockBlockMethod = getLockBlockMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.LockBlockPRequest, alluxio.grpc.LockBlockPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorkerClientService", "LockBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.LockBlockPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.LockBlockPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerClientServiceMethodDescriptorSupplier("LockBlock"))
                  .build();
          }
        }
     }
     return getLockBlockMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getPromoteBlockMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.PromoteBlockPRequest,
      alluxio.grpc.PromoteBlockPResponse> METHOD_PROMOTE_BLOCK = getPromoteBlockMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.PromoteBlockPRequest,
      alluxio.grpc.PromoteBlockPResponse> getPromoteBlockMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.PromoteBlockPRequest,
      alluxio.grpc.PromoteBlockPResponse> getPromoteBlockMethod() {
    return getPromoteBlockMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.PromoteBlockPRequest,
      alluxio.grpc.PromoteBlockPResponse> getPromoteBlockMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.PromoteBlockPRequest, alluxio.grpc.PromoteBlockPResponse> getPromoteBlockMethod;
    if ((getPromoteBlockMethod = BlockWorkerClientServiceGrpc.getPromoteBlockMethod) == null) {
      synchronized (BlockWorkerClientServiceGrpc.class) {
        if ((getPromoteBlockMethod = BlockWorkerClientServiceGrpc.getPromoteBlockMethod) == null) {
          BlockWorkerClientServiceGrpc.getPromoteBlockMethod = getPromoteBlockMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.PromoteBlockPRequest, alluxio.grpc.PromoteBlockPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorkerClientService", "PromoteBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.PromoteBlockPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.PromoteBlockPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerClientServiceMethodDescriptorSupplier("PromoteBlock"))
                  .build();
          }
        }
     }
     return getPromoteBlockMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getRemoveBlockMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.RemoveBlockPRequest,
      alluxio.grpc.RemoveBlockPResponse> METHOD_REMOVE_BLOCK = getRemoveBlockMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RemoveBlockPRequest,
      alluxio.grpc.RemoveBlockPResponse> getRemoveBlockMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.RemoveBlockPRequest,
      alluxio.grpc.RemoveBlockPResponse> getRemoveBlockMethod() {
    return getRemoveBlockMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.RemoveBlockPRequest,
      alluxio.grpc.RemoveBlockPResponse> getRemoveBlockMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.RemoveBlockPRequest, alluxio.grpc.RemoveBlockPResponse> getRemoveBlockMethod;
    if ((getRemoveBlockMethod = BlockWorkerClientServiceGrpc.getRemoveBlockMethod) == null) {
      synchronized (BlockWorkerClientServiceGrpc.class) {
        if ((getRemoveBlockMethod = BlockWorkerClientServiceGrpc.getRemoveBlockMethod) == null) {
          BlockWorkerClientServiceGrpc.getRemoveBlockMethod = getRemoveBlockMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.RemoveBlockPRequest, alluxio.grpc.RemoveBlockPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorkerClientService", "RemoveBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RemoveBlockPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RemoveBlockPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerClientServiceMethodDescriptorSupplier("RemoveBlock"))
                  .build();
          }
        }
     }
     return getRemoveBlockMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getRequestBlockLocationMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.RequestBlockLocationPRequest,
      alluxio.grpc.RequestBlockLocationPResponse> METHOD_REQUEST_BLOCK_LOCATION = getRequestBlockLocationMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RequestBlockLocationPRequest,
      alluxio.grpc.RequestBlockLocationPResponse> getRequestBlockLocationMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.RequestBlockLocationPRequest,
      alluxio.grpc.RequestBlockLocationPResponse> getRequestBlockLocationMethod() {
    return getRequestBlockLocationMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.RequestBlockLocationPRequest,
      alluxio.grpc.RequestBlockLocationPResponse> getRequestBlockLocationMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.RequestBlockLocationPRequest, alluxio.grpc.RequestBlockLocationPResponse> getRequestBlockLocationMethod;
    if ((getRequestBlockLocationMethod = BlockWorkerClientServiceGrpc.getRequestBlockLocationMethod) == null) {
      synchronized (BlockWorkerClientServiceGrpc.class) {
        if ((getRequestBlockLocationMethod = BlockWorkerClientServiceGrpc.getRequestBlockLocationMethod) == null) {
          BlockWorkerClientServiceGrpc.getRequestBlockLocationMethod = getRequestBlockLocationMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.RequestBlockLocationPRequest, alluxio.grpc.RequestBlockLocationPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorkerClientService", "RequestBlockLocation"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RequestBlockLocationPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RequestBlockLocationPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerClientServiceMethodDescriptorSupplier("RequestBlockLocation"))
                  .build();
          }
        }
     }
     return getRequestBlockLocationMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getRequestSpaceMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.RequestSpacePRequest,
      alluxio.grpc.RequestSpacePResponse> METHOD_REQUEST_SPACE = getRequestSpaceMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RequestSpacePRequest,
      alluxio.grpc.RequestSpacePResponse> getRequestSpaceMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.RequestSpacePRequest,
      alluxio.grpc.RequestSpacePResponse> getRequestSpaceMethod() {
    return getRequestSpaceMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.RequestSpacePRequest,
      alluxio.grpc.RequestSpacePResponse> getRequestSpaceMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.RequestSpacePRequest, alluxio.grpc.RequestSpacePResponse> getRequestSpaceMethod;
    if ((getRequestSpaceMethod = BlockWorkerClientServiceGrpc.getRequestSpaceMethod) == null) {
      synchronized (BlockWorkerClientServiceGrpc.class) {
        if ((getRequestSpaceMethod = BlockWorkerClientServiceGrpc.getRequestSpaceMethod) == null) {
          BlockWorkerClientServiceGrpc.getRequestSpaceMethod = getRequestSpaceMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.RequestSpacePRequest, alluxio.grpc.RequestSpacePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorkerClientService", "RequestSpace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RequestSpacePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RequestSpacePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerClientServiceMethodDescriptorSupplier("RequestSpace"))
                  .build();
          }
        }
     }
     return getRequestSpaceMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getSessionBlockHeartbeatMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.SessionBlockHeartbeatPRequest,
      alluxio.grpc.SessionBlockHeartbeatPResponse> METHOD_SESSION_BLOCK_HEARTBEAT = getSessionBlockHeartbeatMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.SessionBlockHeartbeatPRequest,
      alluxio.grpc.SessionBlockHeartbeatPResponse> getSessionBlockHeartbeatMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.SessionBlockHeartbeatPRequest,
      alluxio.grpc.SessionBlockHeartbeatPResponse> getSessionBlockHeartbeatMethod() {
    return getSessionBlockHeartbeatMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.SessionBlockHeartbeatPRequest,
      alluxio.grpc.SessionBlockHeartbeatPResponse> getSessionBlockHeartbeatMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.SessionBlockHeartbeatPRequest, alluxio.grpc.SessionBlockHeartbeatPResponse> getSessionBlockHeartbeatMethod;
    if ((getSessionBlockHeartbeatMethod = BlockWorkerClientServiceGrpc.getSessionBlockHeartbeatMethod) == null) {
      synchronized (BlockWorkerClientServiceGrpc.class) {
        if ((getSessionBlockHeartbeatMethod = BlockWorkerClientServiceGrpc.getSessionBlockHeartbeatMethod) == null) {
          BlockWorkerClientServiceGrpc.getSessionBlockHeartbeatMethod = getSessionBlockHeartbeatMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.SessionBlockHeartbeatPRequest, alluxio.grpc.SessionBlockHeartbeatPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorkerClientService", "SessionBlockHeartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SessionBlockHeartbeatPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SessionBlockHeartbeatPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerClientServiceMethodDescriptorSupplier("SessionBlockHeartbeat"))
                  .build();
          }
        }
     }
     return getSessionBlockHeartbeatMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getUnlockBlockMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.UnlockBlockPRequest,
      alluxio.grpc.UnlockBlockPResponse> METHOD_UNLOCK_BLOCK = getUnlockBlockMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.UnlockBlockPRequest,
      alluxio.grpc.UnlockBlockPResponse> getUnlockBlockMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.UnlockBlockPRequest,
      alluxio.grpc.UnlockBlockPResponse> getUnlockBlockMethod() {
    return getUnlockBlockMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.UnlockBlockPRequest,
      alluxio.grpc.UnlockBlockPResponse> getUnlockBlockMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.UnlockBlockPRequest, alluxio.grpc.UnlockBlockPResponse> getUnlockBlockMethod;
    if ((getUnlockBlockMethod = BlockWorkerClientServiceGrpc.getUnlockBlockMethod) == null) {
      synchronized (BlockWorkerClientServiceGrpc.class) {
        if ((getUnlockBlockMethod = BlockWorkerClientServiceGrpc.getUnlockBlockMethod) == null) {
          BlockWorkerClientServiceGrpc.getUnlockBlockMethod = getUnlockBlockMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.UnlockBlockPRequest, alluxio.grpc.UnlockBlockPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorkerClientService", "UnlockBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UnlockBlockPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UnlockBlockPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerClientServiceMethodDescriptorSupplier("UnlockBlock"))
                  .build();
          }
        }
     }
     return getUnlockBlockMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BlockWorkerClientServiceStub newStub(io.grpc.Channel channel) {
    return new BlockWorkerClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BlockWorkerClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BlockWorkerClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BlockWorkerClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BlockWorkerClientServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class BlockWorkerClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Accesses a block given the block id.
     * </pre>
     */
    public void accessBlock(alluxio.grpc.AccessBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.AccessBlockPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getAccessBlockMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Used to cache a block into Alluxio space; worker will move the temporary block file from session
     * folder to data folder; and update the space usage information related. then update the block
     * information to master.
     * </pre>
     */
    public void cacheBlock(alluxio.grpc.CacheBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CacheBlockPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCacheBlockMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Used to cancel a block which is being written. worker will delete the temporary block file and
     * the location and space information related; then reclaim space allocated to the block.
     * </pre>
     */
    public void cancelBlock(alluxio.grpc.CancelBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CancelBlockPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCancelBlockMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Locks the file in Alluxio's space while the session is reading it. If lock succeeds; the path of
     * the block's file along with the internal lock id of locked block will be returned. If the block's file
     * is not found; FileDoesNotExistException will be thrown.
     * </pre>
     */
    public void lockBlock(alluxio.grpc.LockBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.LockBlockPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getLockBlockMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Used to promote block on under storage layer to top storage layer when there are more than one
     * storage layers in Alluxio's space. return true if the block is successfully promoted; false
     * otherwise.
     * </pre>
     */
    public void promoteBlock(alluxio.grpc.PromoteBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.PromoteBlockPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getPromoteBlockMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Used to remove a block from an Alluxio worker.
     * </pre>
     */
    public void removeBlock(alluxio.grpc.RemoveBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RemoveBlockPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRemoveBlockMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Used to allocate location and space for a new coming block; worker will choose the appropriate
     * storage directory which fits the initial block size by some allocation strategy; and the
     * temporary file path of the block file will be returned. if there is no enough space on Alluxio
     * storage OutOfSpaceException will be thrown; if the file is already being written by the session;
     * FileAlreadyExistsException will be thrown.
     * </pre>
     */
    public void requestBlockLocation(alluxio.grpc.RequestBlockLocationPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RequestBlockLocationPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRequestBlockLocationMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Used to request space for some block file. return true if the worker successfully allocates
     * space for the block on block’s location; false if there is no enough space; if there is no
     * information of the block on worker; FileDoesNotExistException will be thrown.
     * </pre>
     */
    public void requestSpace(alluxio.grpc.RequestSpacePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RequestSpacePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRequestSpaceMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Local session send heartbeat to local worker to keep its temporary folder.
     * </pre>
     */
    public void sessionBlockHeartbeat(alluxio.grpc.SessionBlockHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SessionBlockHeartbeatPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSessionBlockHeartbeatMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Used to unlock a block after the block is accessed; if the block is to be removed; delete the
     * block file. return true if successfully unlock the block; return false if the block is not
     * found or failed to delete the block.
     * </pre>
     */
    public void unlockBlock(alluxio.grpc.UnlockBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UnlockBlockPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getUnlockBlockMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getAccessBlockMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.AccessBlockPRequest,
                alluxio.grpc.AccessBlockPResponse>(
                  this, METHODID_ACCESS_BLOCK)))
          .addMethod(
            getCacheBlockMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CacheBlockPRequest,
                alluxio.grpc.CacheBlockPResponse>(
                  this, METHODID_CACHE_BLOCK)))
          .addMethod(
            getCancelBlockMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CancelBlockPRequest,
                alluxio.grpc.CancelBlockPResponse>(
                  this, METHODID_CANCEL_BLOCK)))
          .addMethod(
            getLockBlockMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.LockBlockPRequest,
                alluxio.grpc.LockBlockPResponse>(
                  this, METHODID_LOCK_BLOCK)))
          .addMethod(
            getPromoteBlockMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.PromoteBlockPRequest,
                alluxio.grpc.PromoteBlockPResponse>(
                  this, METHODID_PROMOTE_BLOCK)))
          .addMethod(
            getRemoveBlockMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RemoveBlockPRequest,
                alluxio.grpc.RemoveBlockPResponse>(
                  this, METHODID_REMOVE_BLOCK)))
          .addMethod(
            getRequestBlockLocationMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RequestBlockLocationPRequest,
                alluxio.grpc.RequestBlockLocationPResponse>(
                  this, METHODID_REQUEST_BLOCK_LOCATION)))
          .addMethod(
            getRequestSpaceMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RequestSpacePRequest,
                alluxio.grpc.RequestSpacePResponse>(
                  this, METHODID_REQUEST_SPACE)))
          .addMethod(
            getSessionBlockHeartbeatMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.SessionBlockHeartbeatPRequest,
                alluxio.grpc.SessionBlockHeartbeatPResponse>(
                  this, METHODID_SESSION_BLOCK_HEARTBEAT)))
          .addMethod(
            getUnlockBlockMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.UnlockBlockPRequest,
                alluxio.grpc.UnlockBlockPResponse>(
                  this, METHODID_UNLOCK_BLOCK)))
          .build();
    }
  }

  /**
   */
  public static final class BlockWorkerClientServiceStub extends io.grpc.stub.AbstractStub<BlockWorkerClientServiceStub> {
    private BlockWorkerClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockWorkerClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockWorkerClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockWorkerClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Accesses a block given the block id.
     * </pre>
     */
    public void accessBlock(alluxio.grpc.AccessBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.AccessBlockPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getAccessBlockMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Used to cache a block into Alluxio space; worker will move the temporary block file from session
     * folder to data folder; and update the space usage information related. then update the block
     * information to master.
     * </pre>
     */
    public void cacheBlock(alluxio.grpc.CacheBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CacheBlockPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCacheBlockMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Used to cancel a block which is being written. worker will delete the temporary block file and
     * the location and space information related; then reclaim space allocated to the block.
     * </pre>
     */
    public void cancelBlock(alluxio.grpc.CancelBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CancelBlockPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCancelBlockMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Locks the file in Alluxio's space while the session is reading it. If lock succeeds; the path of
     * the block's file along with the internal lock id of locked block will be returned. If the block's file
     * is not found; FileDoesNotExistException will be thrown.
     * </pre>
     */
    public void lockBlock(alluxio.grpc.LockBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.LockBlockPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getLockBlockMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Used to promote block on under storage layer to top storage layer when there are more than one
     * storage layers in Alluxio's space. return true if the block is successfully promoted; false
     * otherwise.
     * </pre>
     */
    public void promoteBlock(alluxio.grpc.PromoteBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.PromoteBlockPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getPromoteBlockMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Used to remove a block from an Alluxio worker.
     * </pre>
     */
    public void removeBlock(alluxio.grpc.RemoveBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RemoveBlockPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRemoveBlockMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Used to allocate location and space for a new coming block; worker will choose the appropriate
     * storage directory which fits the initial block size by some allocation strategy; and the
     * temporary file path of the block file will be returned. if there is no enough space on Alluxio
     * storage OutOfSpaceException will be thrown; if the file is already being written by the session;
     * FileAlreadyExistsException will be thrown.
     * </pre>
     */
    public void requestBlockLocation(alluxio.grpc.RequestBlockLocationPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RequestBlockLocationPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRequestBlockLocationMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Used to request space for some block file. return true if the worker successfully allocates
     * space for the block on block’s location; false if there is no enough space; if there is no
     * information of the block on worker; FileDoesNotExistException will be thrown.
     * </pre>
     */
    public void requestSpace(alluxio.grpc.RequestSpacePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RequestSpacePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRequestSpaceMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Local session send heartbeat to local worker to keep its temporary folder.
     * </pre>
     */
    public void sessionBlockHeartbeat(alluxio.grpc.SessionBlockHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SessionBlockHeartbeatPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSessionBlockHeartbeatMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Used to unlock a block after the block is accessed; if the block is to be removed; delete the
     * block file. return true if successfully unlock the block; return false if the block is not
     * found or failed to delete the block.
     * </pre>
     */
    public void unlockBlock(alluxio.grpc.UnlockBlockPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UnlockBlockPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getUnlockBlockMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class BlockWorkerClientServiceBlockingStub extends io.grpc.stub.AbstractStub<BlockWorkerClientServiceBlockingStub> {
    private BlockWorkerClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockWorkerClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockWorkerClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockWorkerClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Accesses a block given the block id.
     * </pre>
     */
    public alluxio.grpc.AccessBlockPResponse accessBlock(alluxio.grpc.AccessBlockPRequest request) {
      return blockingUnaryCall(
          getChannel(), getAccessBlockMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Used to cache a block into Alluxio space; worker will move the temporary block file from session
     * folder to data folder; and update the space usage information related. then update the block
     * information to master.
     * </pre>
     */
    public alluxio.grpc.CacheBlockPResponse cacheBlock(alluxio.grpc.CacheBlockPRequest request) {
      return blockingUnaryCall(
          getChannel(), getCacheBlockMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Used to cancel a block which is being written. worker will delete the temporary block file and
     * the location and space information related; then reclaim space allocated to the block.
     * </pre>
     */
    public alluxio.grpc.CancelBlockPResponse cancelBlock(alluxio.grpc.CancelBlockPRequest request) {
      return blockingUnaryCall(
          getChannel(), getCancelBlockMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Locks the file in Alluxio's space while the session is reading it. If lock succeeds; the path of
     * the block's file along with the internal lock id of locked block will be returned. If the block's file
     * is not found; FileDoesNotExistException will be thrown.
     * </pre>
     */
    public alluxio.grpc.LockBlockPResponse lockBlock(alluxio.grpc.LockBlockPRequest request) {
      return blockingUnaryCall(
          getChannel(), getLockBlockMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Used to promote block on under storage layer to top storage layer when there are more than one
     * storage layers in Alluxio's space. return true if the block is successfully promoted; false
     * otherwise.
     * </pre>
     */
    public alluxio.grpc.PromoteBlockPResponse promoteBlock(alluxio.grpc.PromoteBlockPRequest request) {
      return blockingUnaryCall(
          getChannel(), getPromoteBlockMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Used to remove a block from an Alluxio worker.
     * </pre>
     */
    public alluxio.grpc.RemoveBlockPResponse removeBlock(alluxio.grpc.RemoveBlockPRequest request) {
      return blockingUnaryCall(
          getChannel(), getRemoveBlockMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Used to allocate location and space for a new coming block; worker will choose the appropriate
     * storage directory which fits the initial block size by some allocation strategy; and the
     * temporary file path of the block file will be returned. if there is no enough space on Alluxio
     * storage OutOfSpaceException will be thrown; if the file is already being written by the session;
     * FileAlreadyExistsException will be thrown.
     * </pre>
     */
    public alluxio.grpc.RequestBlockLocationPResponse requestBlockLocation(alluxio.grpc.RequestBlockLocationPRequest request) {
      return blockingUnaryCall(
          getChannel(), getRequestBlockLocationMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Used to request space for some block file. return true if the worker successfully allocates
     * space for the block on block’s location; false if there is no enough space; if there is no
     * information of the block on worker; FileDoesNotExistException will be thrown.
     * </pre>
     */
    public alluxio.grpc.RequestSpacePResponse requestSpace(alluxio.grpc.RequestSpacePRequest request) {
      return blockingUnaryCall(
          getChannel(), getRequestSpaceMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Local session send heartbeat to local worker to keep its temporary folder.
     * </pre>
     */
    public alluxio.grpc.SessionBlockHeartbeatPResponse sessionBlockHeartbeat(alluxio.grpc.SessionBlockHeartbeatPRequest request) {
      return blockingUnaryCall(
          getChannel(), getSessionBlockHeartbeatMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Used to unlock a block after the block is accessed; if the block is to be removed; delete the
     * block file. return true if successfully unlock the block; return false if the block is not
     * found or failed to delete the block.
     * </pre>
     */
    public alluxio.grpc.UnlockBlockPResponse unlockBlock(alluxio.grpc.UnlockBlockPRequest request) {
      return blockingUnaryCall(
          getChannel(), getUnlockBlockMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class BlockWorkerClientServiceFutureStub extends io.grpc.stub.AbstractStub<BlockWorkerClientServiceFutureStub> {
    private BlockWorkerClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockWorkerClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockWorkerClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockWorkerClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Accesses a block given the block id.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.AccessBlockPResponse> accessBlock(
        alluxio.grpc.AccessBlockPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getAccessBlockMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Used to cache a block into Alluxio space; worker will move the temporary block file from session
     * folder to data folder; and update the space usage information related. then update the block
     * information to master.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CacheBlockPResponse> cacheBlock(
        alluxio.grpc.CacheBlockPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCacheBlockMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Used to cancel a block which is being written. worker will delete the temporary block file and
     * the location and space information related; then reclaim space allocated to the block.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CancelBlockPResponse> cancelBlock(
        alluxio.grpc.CancelBlockPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCancelBlockMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Locks the file in Alluxio's space while the session is reading it. If lock succeeds; the path of
     * the block's file along with the internal lock id of locked block will be returned. If the block's file
     * is not found; FileDoesNotExistException will be thrown.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.LockBlockPResponse> lockBlock(
        alluxio.grpc.LockBlockPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getLockBlockMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Used to promote block on under storage layer to top storage layer when there are more than one
     * storage layers in Alluxio's space. return true if the block is successfully promoted; false
     * otherwise.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.PromoteBlockPResponse> promoteBlock(
        alluxio.grpc.PromoteBlockPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getPromoteBlockMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Used to remove a block from an Alluxio worker.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RemoveBlockPResponse> removeBlock(
        alluxio.grpc.RemoveBlockPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRemoveBlockMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Used to allocate location and space for a new coming block; worker will choose the appropriate
     * storage directory which fits the initial block size by some allocation strategy; and the
     * temporary file path of the block file will be returned. if there is no enough space on Alluxio
     * storage OutOfSpaceException will be thrown; if the file is already being written by the session;
     * FileAlreadyExistsException will be thrown.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RequestBlockLocationPResponse> requestBlockLocation(
        alluxio.grpc.RequestBlockLocationPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRequestBlockLocationMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Used to request space for some block file. return true if the worker successfully allocates
     * space for the block on block’s location; false if there is no enough space; if there is no
     * information of the block on worker; FileDoesNotExistException will be thrown.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RequestSpacePResponse> requestSpace(
        alluxio.grpc.RequestSpacePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRequestSpaceMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Local session send heartbeat to local worker to keep its temporary folder.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.SessionBlockHeartbeatPResponse> sessionBlockHeartbeat(
        alluxio.grpc.SessionBlockHeartbeatPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSessionBlockHeartbeatMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Used to unlock a block after the block is accessed; if the block is to be removed; delete the
     * block file. return true if successfully unlock the block; return false if the block is not
     * found or failed to delete the block.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.UnlockBlockPResponse> unlockBlock(
        alluxio.grpc.UnlockBlockPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getUnlockBlockMethodHelper(), getCallOptions()), request);
    }
  }

  private static final int METHODID_ACCESS_BLOCK = 0;
  private static final int METHODID_CACHE_BLOCK = 1;
  private static final int METHODID_CANCEL_BLOCK = 2;
  private static final int METHODID_LOCK_BLOCK = 3;
  private static final int METHODID_PROMOTE_BLOCK = 4;
  private static final int METHODID_REMOVE_BLOCK = 5;
  private static final int METHODID_REQUEST_BLOCK_LOCATION = 6;
  private static final int METHODID_REQUEST_SPACE = 7;
  private static final int METHODID_SESSION_BLOCK_HEARTBEAT = 8;
  private static final int METHODID_UNLOCK_BLOCK = 9;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BlockWorkerClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BlockWorkerClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_ACCESS_BLOCK:
          serviceImpl.accessBlock((alluxio.grpc.AccessBlockPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.AccessBlockPResponse>) responseObserver);
          break;
        case METHODID_CACHE_BLOCK:
          serviceImpl.cacheBlock((alluxio.grpc.CacheBlockPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CacheBlockPResponse>) responseObserver);
          break;
        case METHODID_CANCEL_BLOCK:
          serviceImpl.cancelBlock((alluxio.grpc.CancelBlockPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CancelBlockPResponse>) responseObserver);
          break;
        case METHODID_LOCK_BLOCK:
          serviceImpl.lockBlock((alluxio.grpc.LockBlockPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.LockBlockPResponse>) responseObserver);
          break;
        case METHODID_PROMOTE_BLOCK:
          serviceImpl.promoteBlock((alluxio.grpc.PromoteBlockPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.PromoteBlockPResponse>) responseObserver);
          break;
        case METHODID_REMOVE_BLOCK:
          serviceImpl.removeBlock((alluxio.grpc.RemoveBlockPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RemoveBlockPResponse>) responseObserver);
          break;
        case METHODID_REQUEST_BLOCK_LOCATION:
          serviceImpl.requestBlockLocation((alluxio.grpc.RequestBlockLocationPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RequestBlockLocationPResponse>) responseObserver);
          break;
        case METHODID_REQUEST_SPACE:
          serviceImpl.requestSpace((alluxio.grpc.RequestSpacePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RequestSpacePResponse>) responseObserver);
          break;
        case METHODID_SESSION_BLOCK_HEARTBEAT:
          serviceImpl.sessionBlockHeartbeat((alluxio.grpc.SessionBlockHeartbeatPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.SessionBlockHeartbeatPResponse>) responseObserver);
          break;
        case METHODID_UNLOCK_BLOCK:
          serviceImpl.unlockBlock((alluxio.grpc.UnlockBlockPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.UnlockBlockPResponse>) responseObserver);
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

  private static abstract class BlockWorkerClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BlockWorkerClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.BlockWorkerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BlockWorkerClientService");
    }
  }

  private static final class BlockWorkerClientServiceFileDescriptorSupplier
      extends BlockWorkerClientServiceBaseDescriptorSupplier {
    BlockWorkerClientServiceFileDescriptorSupplier() {}
  }

  private static final class BlockWorkerClientServiceMethodDescriptorSupplier
      extends BlockWorkerClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BlockWorkerClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (BlockWorkerClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BlockWorkerClientServiceFileDescriptorSupplier())
              .addMethod(getAccessBlockMethodHelper())
              .addMethod(getCacheBlockMethodHelper())
              .addMethod(getCancelBlockMethodHelper())
              .addMethod(getLockBlockMethodHelper())
              .addMethod(getPromoteBlockMethodHelper())
              .addMethod(getRemoveBlockMethodHelper())
              .addMethod(getRequestBlockLocationMethodHelper())
              .addMethod(getRequestSpaceMethodHelper())
              .addMethod(getSessionBlockHeartbeatMethodHelper())
              .addMethod(getUnlockBlockMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
