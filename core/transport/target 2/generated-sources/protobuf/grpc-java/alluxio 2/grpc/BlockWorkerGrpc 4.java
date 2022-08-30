package alluxio.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * The block worker service
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.37.0)",
    comments = "Source: grpc/block_worker.proto")
public final class BlockWorkerGrpc {

  private BlockWorkerGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.block.BlockWorker";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ReadRequest,
      alluxio.grpc.ReadResponse> getReadBlockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReadBlock",
      requestType = alluxio.grpc.ReadRequest.class,
      responseType = alluxio.grpc.ReadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<alluxio.grpc.ReadRequest,
      alluxio.grpc.ReadResponse> getReadBlockMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.ReadRequest, alluxio.grpc.ReadResponse> getReadBlockMethod;
    if ((getReadBlockMethod = BlockWorkerGrpc.getReadBlockMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getReadBlockMethod = BlockWorkerGrpc.getReadBlockMethod) == null) {
          BlockWorkerGrpc.getReadBlockMethod = getReadBlockMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.ReadRequest, alluxio.grpc.ReadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReadBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ReadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("ReadBlock"))
              .build();
        }
      }
    }
    return getReadBlockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.WriteRequest,
      alluxio.grpc.WriteResponse> getWriteBlockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WriteBlock",
      requestType = alluxio.grpc.WriteRequest.class,
      responseType = alluxio.grpc.WriteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<alluxio.grpc.WriteRequest,
      alluxio.grpc.WriteResponse> getWriteBlockMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.WriteRequest, alluxio.grpc.WriteResponse> getWriteBlockMethod;
    if ((getWriteBlockMethod = BlockWorkerGrpc.getWriteBlockMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getWriteBlockMethod = BlockWorkerGrpc.getWriteBlockMethod) == null) {
          BlockWorkerGrpc.getWriteBlockMethod = getWriteBlockMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.WriteRequest, alluxio.grpc.WriteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WriteBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.WriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.WriteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("WriteBlock"))
              .build();
        }
      }
    }
    return getWriteBlockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.OpenLocalBlockRequest,
      alluxio.grpc.OpenLocalBlockResponse> getOpenLocalBlockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "OpenLocalBlock",
      requestType = alluxio.grpc.OpenLocalBlockRequest.class,
      responseType = alluxio.grpc.OpenLocalBlockResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<alluxio.grpc.OpenLocalBlockRequest,
      alluxio.grpc.OpenLocalBlockResponse> getOpenLocalBlockMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.OpenLocalBlockRequest, alluxio.grpc.OpenLocalBlockResponse> getOpenLocalBlockMethod;
    if ((getOpenLocalBlockMethod = BlockWorkerGrpc.getOpenLocalBlockMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getOpenLocalBlockMethod = BlockWorkerGrpc.getOpenLocalBlockMethod) == null) {
          BlockWorkerGrpc.getOpenLocalBlockMethod = getOpenLocalBlockMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.OpenLocalBlockRequest, alluxio.grpc.OpenLocalBlockResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "OpenLocalBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.OpenLocalBlockRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.OpenLocalBlockResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("OpenLocalBlock"))
              .build();
        }
      }
    }
    return getOpenLocalBlockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CreateLocalBlockRequest,
      alluxio.grpc.CreateLocalBlockResponse> getCreateLocalBlockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateLocalBlock",
      requestType = alluxio.grpc.CreateLocalBlockRequest.class,
      responseType = alluxio.grpc.CreateLocalBlockResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CreateLocalBlockRequest,
      alluxio.grpc.CreateLocalBlockResponse> getCreateLocalBlockMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CreateLocalBlockRequest, alluxio.grpc.CreateLocalBlockResponse> getCreateLocalBlockMethod;
    if ((getCreateLocalBlockMethod = BlockWorkerGrpc.getCreateLocalBlockMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getCreateLocalBlockMethod = BlockWorkerGrpc.getCreateLocalBlockMethod) == null) {
          BlockWorkerGrpc.getCreateLocalBlockMethod = getCreateLocalBlockMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.CreateLocalBlockRequest, alluxio.grpc.CreateLocalBlockResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateLocalBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateLocalBlockRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateLocalBlockResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("CreateLocalBlock"))
              .build();
        }
      }
    }
    return getCreateLocalBlockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.AsyncCacheRequest,
      alluxio.grpc.AsyncCacheResponse> getAsyncCacheMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AsyncCache",
      requestType = alluxio.grpc.AsyncCacheRequest.class,
      responseType = alluxio.grpc.AsyncCacheResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.AsyncCacheRequest,
      alluxio.grpc.AsyncCacheResponse> getAsyncCacheMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.AsyncCacheRequest, alluxio.grpc.AsyncCacheResponse> getAsyncCacheMethod;
    if ((getAsyncCacheMethod = BlockWorkerGrpc.getAsyncCacheMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getAsyncCacheMethod = BlockWorkerGrpc.getAsyncCacheMethod) == null) {
          BlockWorkerGrpc.getAsyncCacheMethod = getAsyncCacheMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.AsyncCacheRequest, alluxio.grpc.AsyncCacheResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AsyncCache"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.AsyncCacheRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.AsyncCacheResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("AsyncCache"))
              .build();
        }
      }
    }
    return getAsyncCacheMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CacheRequest,
      alluxio.grpc.CacheResponse> getCacheMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Cache",
      requestType = alluxio.grpc.CacheRequest.class,
      responseType = alluxio.grpc.CacheResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CacheRequest,
      alluxio.grpc.CacheResponse> getCacheMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CacheRequest, alluxio.grpc.CacheResponse> getCacheMethod;
    if ((getCacheMethod = BlockWorkerGrpc.getCacheMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getCacheMethod = BlockWorkerGrpc.getCacheMethod) == null) {
          BlockWorkerGrpc.getCacheMethod = getCacheMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.CacheRequest, alluxio.grpc.CacheResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Cache"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CacheRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CacheResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("Cache"))
              .build();
        }
      }
    }
    return getCacheMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.LoadRequest,
      alluxio.grpc.LoadResponse> getLoadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Load",
      requestType = alluxio.grpc.LoadRequest.class,
      responseType = alluxio.grpc.LoadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.LoadRequest,
      alluxio.grpc.LoadResponse> getLoadMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.LoadRequest, alluxio.grpc.LoadResponse> getLoadMethod;
    if ((getLoadMethod = BlockWorkerGrpc.getLoadMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getLoadMethod = BlockWorkerGrpc.getLoadMethod) == null) {
          BlockWorkerGrpc.getLoadMethod = getLoadMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.LoadRequest, alluxio.grpc.LoadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Load"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.LoadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.LoadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("Load"))
              .build();
        }
      }
    }
    return getLoadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RemoveBlockRequest,
      alluxio.grpc.RemoveBlockResponse> getRemoveBlockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RemoveBlock",
      requestType = alluxio.grpc.RemoveBlockRequest.class,
      responseType = alluxio.grpc.RemoveBlockResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.RemoveBlockRequest,
      alluxio.grpc.RemoveBlockResponse> getRemoveBlockMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.RemoveBlockRequest, alluxio.grpc.RemoveBlockResponse> getRemoveBlockMethod;
    if ((getRemoveBlockMethod = BlockWorkerGrpc.getRemoveBlockMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getRemoveBlockMethod = BlockWorkerGrpc.getRemoveBlockMethod) == null) {
          BlockWorkerGrpc.getRemoveBlockMethod = getRemoveBlockMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.RemoveBlockRequest, alluxio.grpc.RemoveBlockResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RemoveBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RemoveBlockRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RemoveBlockResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("RemoveBlock"))
              .build();
        }
      }
    }
    return getRemoveBlockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.MoveBlockRequest,
      alluxio.grpc.MoveBlockResponse> getMoveBlockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MoveBlock",
      requestType = alluxio.grpc.MoveBlockRequest.class,
      responseType = alluxio.grpc.MoveBlockResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.MoveBlockRequest,
      alluxio.grpc.MoveBlockResponse> getMoveBlockMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.MoveBlockRequest, alluxio.grpc.MoveBlockResponse> getMoveBlockMethod;
    if ((getMoveBlockMethod = BlockWorkerGrpc.getMoveBlockMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getMoveBlockMethod = BlockWorkerGrpc.getMoveBlockMethod) == null) {
          BlockWorkerGrpc.getMoveBlockMethod = getMoveBlockMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.MoveBlockRequest, alluxio.grpc.MoveBlockResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MoveBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.MoveBlockRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.MoveBlockResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("MoveBlock"))
              .build();
        }
      }
    }
    return getMoveBlockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ClearMetricsRequest,
      alluxio.grpc.ClearMetricsResponse> getClearMetricsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ClearMetrics",
      requestType = alluxio.grpc.ClearMetricsRequest.class,
      responseType = alluxio.grpc.ClearMetricsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.ClearMetricsRequest,
      alluxio.grpc.ClearMetricsResponse> getClearMetricsMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.ClearMetricsRequest, alluxio.grpc.ClearMetricsResponse> getClearMetricsMethod;
    if ((getClearMetricsMethod = BlockWorkerGrpc.getClearMetricsMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getClearMetricsMethod = BlockWorkerGrpc.getClearMetricsMethod) == null) {
          BlockWorkerGrpc.getClearMetricsMethod = getClearMetricsMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.ClearMetricsRequest, alluxio.grpc.ClearMetricsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ClearMetrics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ClearMetricsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ClearMetricsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("ClearMetrics"))
              .build();
        }
      }
    }
    return getClearMetricsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BlockWorkerStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BlockWorkerStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BlockWorkerStub>() {
        @java.lang.Override
        public BlockWorkerStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BlockWorkerStub(channel, callOptions);
        }
      };
    return BlockWorkerStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BlockWorkerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BlockWorkerBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BlockWorkerBlockingStub>() {
        @java.lang.Override
        public BlockWorkerBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BlockWorkerBlockingStub(channel, callOptions);
        }
      };
    return BlockWorkerBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BlockWorkerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BlockWorkerFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BlockWorkerFutureStub>() {
        @java.lang.Override
        public BlockWorkerFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BlockWorkerFutureStub(channel, callOptions);
        }
      };
    return BlockWorkerFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * The block worker service
   * </pre>
   */
  public static abstract class BlockWorkerImplBase implements io.grpc.BindableService {

    /**
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.ReadRequest> readBlock(
        io.grpc.stub.StreamObserver<alluxio.grpc.ReadResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getReadBlockMethod(), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.WriteRequest> writeBlock(
        io.grpc.stub.StreamObserver<alluxio.grpc.WriteResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getWriteBlockMethod(), responseObserver);
    }

    /**
     * <pre>
     * Replaces ShortCircuitBlockReadHandler.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.OpenLocalBlockRequest> openLocalBlock(
        io.grpc.stub.StreamObserver<alluxio.grpc.OpenLocalBlockResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getOpenLocalBlockMethod(), responseObserver);
    }

    /**
     * <pre>
     * Replaces ShortCircuitBlockWriteHandler.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.CreateLocalBlockRequest> createLocalBlock(
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateLocalBlockResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getCreateLocalBlockMethod(), responseObserver);
    }

    /**
     */
    public void asyncCache(alluxio.grpc.AsyncCacheRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.AsyncCacheResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAsyncCacheMethod(), responseObserver);
    }

    /**
     */
    public void cache(alluxio.grpc.CacheRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CacheResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCacheMethod(), responseObserver);
    }

    /**
     */
    public void load(alluxio.grpc.LoadRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.LoadResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLoadMethod(), responseObserver);
    }

    /**
     */
    public void removeBlock(alluxio.grpc.RemoveBlockRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RemoveBlockResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRemoveBlockMethod(), responseObserver);
    }

    /**
     */
    public void moveBlock(alluxio.grpc.MoveBlockRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MoveBlockResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMoveBlockMethod(), responseObserver);
    }

    /**
     * <pre>
     * TODO(lu) Move to metrics worker
     * </pre>
     */
    public void clearMetrics(alluxio.grpc.ClearMetricsRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ClearMetricsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getClearMetricsMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getReadBlockMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                alluxio.grpc.ReadRequest,
                alluxio.grpc.ReadResponse>(
                  this, METHODID_READ_BLOCK)))
          .addMethod(
            getWriteBlockMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                alluxio.grpc.WriteRequest,
                alluxio.grpc.WriteResponse>(
                  this, METHODID_WRITE_BLOCK)))
          .addMethod(
            getOpenLocalBlockMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                alluxio.grpc.OpenLocalBlockRequest,
                alluxio.grpc.OpenLocalBlockResponse>(
                  this, METHODID_OPEN_LOCAL_BLOCK)))
          .addMethod(
            getCreateLocalBlockMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                alluxio.grpc.CreateLocalBlockRequest,
                alluxio.grpc.CreateLocalBlockResponse>(
                  this, METHODID_CREATE_LOCAL_BLOCK)))
          .addMethod(
            getAsyncCacheMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.AsyncCacheRequest,
                alluxio.grpc.AsyncCacheResponse>(
                  this, METHODID_ASYNC_CACHE)))
          .addMethod(
            getCacheMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CacheRequest,
                alluxio.grpc.CacheResponse>(
                  this, METHODID_CACHE)))
          .addMethod(
            getLoadMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.LoadRequest,
                alluxio.grpc.LoadResponse>(
                  this, METHODID_LOAD)))
          .addMethod(
            getRemoveBlockMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RemoveBlockRequest,
                alluxio.grpc.RemoveBlockResponse>(
                  this, METHODID_REMOVE_BLOCK)))
          .addMethod(
            getMoveBlockMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.MoveBlockRequest,
                alluxio.grpc.MoveBlockResponse>(
                  this, METHODID_MOVE_BLOCK)))
          .addMethod(
            getClearMetricsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.ClearMetricsRequest,
                alluxio.grpc.ClearMetricsResponse>(
                  this, METHODID_CLEAR_METRICS)))
          .build();
    }
  }

  /**
   * <pre>
   * The block worker service
   * </pre>
   */
  public static final class BlockWorkerStub extends io.grpc.stub.AbstractAsyncStub<BlockWorkerStub> {
    private BlockWorkerStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockWorkerStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BlockWorkerStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.ReadRequest> readBlock(
        io.grpc.stub.StreamObserver<alluxio.grpc.ReadResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getReadBlockMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.WriteRequest> writeBlock(
        io.grpc.stub.StreamObserver<alluxio.grpc.WriteResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getWriteBlockMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     * Replaces ShortCircuitBlockReadHandler.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.OpenLocalBlockRequest> openLocalBlock(
        io.grpc.stub.StreamObserver<alluxio.grpc.OpenLocalBlockResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getOpenLocalBlockMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     * Replaces ShortCircuitBlockWriteHandler.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.CreateLocalBlockRequest> createLocalBlock(
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateLocalBlockResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getCreateLocalBlockMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void asyncCache(alluxio.grpc.AsyncCacheRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.AsyncCacheResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAsyncCacheMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cache(alluxio.grpc.CacheRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CacheResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCacheMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void load(alluxio.grpc.LoadRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.LoadResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLoadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void removeBlock(alluxio.grpc.RemoveBlockRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RemoveBlockResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRemoveBlockMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void moveBlock(alluxio.grpc.MoveBlockRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MoveBlockResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMoveBlockMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * TODO(lu) Move to metrics worker
     * </pre>
     */
    public void clearMetrics(alluxio.grpc.ClearMetricsRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ClearMetricsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getClearMetricsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The block worker service
   * </pre>
   */
  public static final class BlockWorkerBlockingStub extends io.grpc.stub.AbstractBlockingStub<BlockWorkerBlockingStub> {
    private BlockWorkerBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockWorkerBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BlockWorkerBlockingStub(channel, callOptions);
    }

    /**
     */
    public alluxio.grpc.AsyncCacheResponse asyncCache(alluxio.grpc.AsyncCacheRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAsyncCacheMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.CacheResponse cache(alluxio.grpc.CacheRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCacheMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.LoadResponse load(alluxio.grpc.LoadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLoadMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.RemoveBlockResponse removeBlock(alluxio.grpc.RemoveBlockRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRemoveBlockMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.MoveBlockResponse moveBlock(alluxio.grpc.MoveBlockRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMoveBlockMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * TODO(lu) Move to metrics worker
     * </pre>
     */
    public alluxio.grpc.ClearMetricsResponse clearMetrics(alluxio.grpc.ClearMetricsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getClearMetricsMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The block worker service
   * </pre>
   */
  public static final class BlockWorkerFutureStub extends io.grpc.stub.AbstractFutureStub<BlockWorkerFutureStub> {
    private BlockWorkerFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockWorkerFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BlockWorkerFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.AsyncCacheResponse> asyncCache(
        alluxio.grpc.AsyncCacheRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAsyncCacheMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CacheResponse> cache(
        alluxio.grpc.CacheRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCacheMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.LoadResponse> load(
        alluxio.grpc.LoadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLoadMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RemoveBlockResponse> removeBlock(
        alluxio.grpc.RemoveBlockRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRemoveBlockMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.MoveBlockResponse> moveBlock(
        alluxio.grpc.MoveBlockRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMoveBlockMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * TODO(lu) Move to metrics worker
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.ClearMetricsResponse> clearMetrics(
        alluxio.grpc.ClearMetricsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getClearMetricsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_ASYNC_CACHE = 0;
  private static final int METHODID_CACHE = 1;
  private static final int METHODID_LOAD = 2;
  private static final int METHODID_REMOVE_BLOCK = 3;
  private static final int METHODID_MOVE_BLOCK = 4;
  private static final int METHODID_CLEAR_METRICS = 5;
  private static final int METHODID_READ_BLOCK = 6;
  private static final int METHODID_WRITE_BLOCK = 7;
  private static final int METHODID_OPEN_LOCAL_BLOCK = 8;
  private static final int METHODID_CREATE_LOCAL_BLOCK = 9;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BlockWorkerImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BlockWorkerImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_ASYNC_CACHE:
          serviceImpl.asyncCache((alluxio.grpc.AsyncCacheRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.AsyncCacheResponse>) responseObserver);
          break;
        case METHODID_CACHE:
          serviceImpl.cache((alluxio.grpc.CacheRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CacheResponse>) responseObserver);
          break;
        case METHODID_LOAD:
          serviceImpl.load((alluxio.grpc.LoadRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.LoadResponse>) responseObserver);
          break;
        case METHODID_REMOVE_BLOCK:
          serviceImpl.removeBlock((alluxio.grpc.RemoveBlockRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RemoveBlockResponse>) responseObserver);
          break;
        case METHODID_MOVE_BLOCK:
          serviceImpl.moveBlock((alluxio.grpc.MoveBlockRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.MoveBlockResponse>) responseObserver);
          break;
        case METHODID_CLEAR_METRICS:
          serviceImpl.clearMetrics((alluxio.grpc.ClearMetricsRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.ClearMetricsResponse>) responseObserver);
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
        case METHODID_READ_BLOCK:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.readBlock(
              (io.grpc.stub.StreamObserver<alluxio.grpc.ReadResponse>) responseObserver);
        case METHODID_WRITE_BLOCK:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.writeBlock(
              (io.grpc.stub.StreamObserver<alluxio.grpc.WriteResponse>) responseObserver);
        case METHODID_OPEN_LOCAL_BLOCK:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.openLocalBlock(
              (io.grpc.stub.StreamObserver<alluxio.grpc.OpenLocalBlockResponse>) responseObserver);
        case METHODID_CREATE_LOCAL_BLOCK:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.createLocalBlock(
              (io.grpc.stub.StreamObserver<alluxio.grpc.CreateLocalBlockResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class BlockWorkerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BlockWorkerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.BlockWorkerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BlockWorker");
    }
  }

  private static final class BlockWorkerFileDescriptorSupplier
      extends BlockWorkerBaseDescriptorSupplier {
    BlockWorkerFileDescriptorSupplier() {}
  }

  private static final class BlockWorkerMethodDescriptorSupplier
      extends BlockWorkerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BlockWorkerMethodDescriptorSupplier(String methodName) {
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
      synchronized (BlockWorkerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BlockWorkerFileDescriptorSupplier())
              .addMethod(getReadBlockMethod())
              .addMethod(getWriteBlockMethod())
              .addMethod(getOpenLocalBlockMethod())
              .addMethod(getCreateLocalBlockMethod())
              .addMethod(getAsyncCacheMethod())
              .addMethod(getCacheMethod())
              .addMethod(getLoadMethod())
              .addMethod(getRemoveBlockMethod())
              .addMethod(getMoveBlockMethod())
              .addMethod(getClearMetricsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
