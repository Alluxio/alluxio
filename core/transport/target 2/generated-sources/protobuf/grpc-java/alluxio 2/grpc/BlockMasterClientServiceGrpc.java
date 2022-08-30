package alluxio.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 **
 * This interface contains block master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.37.0)",
    comments = "Source: grpc/block_master.proto")
public final class BlockMasterClientServiceGrpc {

  private BlockMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.block.BlockMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetBlockInfoPRequest,
      alluxio.grpc.GetBlockInfoPResponse> getGetBlockInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetBlockInfo",
      requestType = alluxio.grpc.GetBlockInfoPRequest.class,
      responseType = alluxio.grpc.GetBlockInfoPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetBlockInfoPRequest,
      alluxio.grpc.GetBlockInfoPResponse> getGetBlockInfoMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetBlockInfoPRequest, alluxio.grpc.GetBlockInfoPResponse> getGetBlockInfoMethod;
    if ((getGetBlockInfoMethod = BlockMasterClientServiceGrpc.getGetBlockInfoMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetBlockInfoMethod = BlockMasterClientServiceGrpc.getGetBlockInfoMethod) == null) {
          BlockMasterClientServiceGrpc.getGetBlockInfoMethod = getGetBlockInfoMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetBlockInfoPRequest, alluxio.grpc.GetBlockInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetBlockInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetBlockInfoPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetBlockInfoPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetBlockInfo"))
              .build();
        }
      }
    }
    return getGetBlockInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetBlockMasterInfoPOptions,
      alluxio.grpc.GetBlockMasterInfoPResponse> getGetBlockMasterInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetBlockMasterInfo",
      requestType = alluxio.grpc.GetBlockMasterInfoPOptions.class,
      responseType = alluxio.grpc.GetBlockMasterInfoPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetBlockMasterInfoPOptions,
      alluxio.grpc.GetBlockMasterInfoPResponse> getGetBlockMasterInfoMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetBlockMasterInfoPOptions, alluxio.grpc.GetBlockMasterInfoPResponse> getGetBlockMasterInfoMethod;
    if ((getGetBlockMasterInfoMethod = BlockMasterClientServiceGrpc.getGetBlockMasterInfoMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetBlockMasterInfoMethod = BlockMasterClientServiceGrpc.getGetBlockMasterInfoMethod) == null) {
          BlockMasterClientServiceGrpc.getGetBlockMasterInfoMethod = getGetBlockMasterInfoMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetBlockMasterInfoPOptions, alluxio.grpc.GetBlockMasterInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetBlockMasterInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetBlockMasterInfoPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetBlockMasterInfoPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetBlockMasterInfo"))
              .build();
        }
      }
    }
    return getGetBlockMasterInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetCapacityBytesPOptions,
      alluxio.grpc.GetCapacityBytesPResponse> getGetCapacityBytesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCapacityBytes",
      requestType = alluxio.grpc.GetCapacityBytesPOptions.class,
      responseType = alluxio.grpc.GetCapacityBytesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetCapacityBytesPOptions,
      alluxio.grpc.GetCapacityBytesPResponse> getGetCapacityBytesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetCapacityBytesPOptions, alluxio.grpc.GetCapacityBytesPResponse> getGetCapacityBytesMethod;
    if ((getGetCapacityBytesMethod = BlockMasterClientServiceGrpc.getGetCapacityBytesMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetCapacityBytesMethod = BlockMasterClientServiceGrpc.getGetCapacityBytesMethod) == null) {
          BlockMasterClientServiceGrpc.getGetCapacityBytesMethod = getGetCapacityBytesMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetCapacityBytesPOptions, alluxio.grpc.GetCapacityBytesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCapacityBytes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetCapacityBytesPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetCapacityBytesPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetCapacityBytes"))
              .build();
        }
      }
    }
    return getGetCapacityBytesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetUsedBytesPOptions,
      alluxio.grpc.GetUsedBytesPResponse> getGetUsedBytesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetUsedBytes",
      requestType = alluxio.grpc.GetUsedBytesPOptions.class,
      responseType = alluxio.grpc.GetUsedBytesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetUsedBytesPOptions,
      alluxio.grpc.GetUsedBytesPResponse> getGetUsedBytesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetUsedBytesPOptions, alluxio.grpc.GetUsedBytesPResponse> getGetUsedBytesMethod;
    if ((getGetUsedBytesMethod = BlockMasterClientServiceGrpc.getGetUsedBytesMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetUsedBytesMethod = BlockMasterClientServiceGrpc.getGetUsedBytesMethod) == null) {
          BlockMasterClientServiceGrpc.getGetUsedBytesMethod = getGetUsedBytesMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetUsedBytesPOptions, alluxio.grpc.GetUsedBytesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetUsedBytes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetUsedBytesPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetUsedBytesPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetUsedBytes"))
              .build();
        }
      }
    }
    return getGetUsedBytesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerInfoListPOptions,
      alluxio.grpc.GetWorkerInfoListPResponse> getGetWorkerInfoListMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetWorkerInfoList",
      requestType = alluxio.grpc.GetWorkerInfoListPOptions.class,
      responseType = alluxio.grpc.GetWorkerInfoListPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerInfoListPOptions,
      alluxio.grpc.GetWorkerInfoListPResponse> getGetWorkerInfoListMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerInfoListPOptions, alluxio.grpc.GetWorkerInfoListPResponse> getGetWorkerInfoListMethod;
    if ((getGetWorkerInfoListMethod = BlockMasterClientServiceGrpc.getGetWorkerInfoListMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetWorkerInfoListMethod = BlockMasterClientServiceGrpc.getGetWorkerInfoListMethod) == null) {
          BlockMasterClientServiceGrpc.getGetWorkerInfoListMethod = getGetWorkerInfoListMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetWorkerInfoListPOptions, alluxio.grpc.GetWorkerInfoListPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetWorkerInfoList"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetWorkerInfoListPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetWorkerInfoListPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetWorkerInfoList"))
              .build();
        }
      }
    }
    return getGetWorkerInfoListMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerReportPOptions,
      alluxio.grpc.GetWorkerInfoListPResponse> getGetWorkerReportMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetWorkerReport",
      requestType = alluxio.grpc.GetWorkerReportPOptions.class,
      responseType = alluxio.grpc.GetWorkerInfoListPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerReportPOptions,
      alluxio.grpc.GetWorkerInfoListPResponse> getGetWorkerReportMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerReportPOptions, alluxio.grpc.GetWorkerInfoListPResponse> getGetWorkerReportMethod;
    if ((getGetWorkerReportMethod = BlockMasterClientServiceGrpc.getGetWorkerReportMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetWorkerReportMethod = BlockMasterClientServiceGrpc.getGetWorkerReportMethod) == null) {
          BlockMasterClientServiceGrpc.getGetWorkerReportMethod = getGetWorkerReportMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetWorkerReportPOptions, alluxio.grpc.GetWorkerInfoListPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetWorkerReport"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetWorkerReportPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetWorkerInfoListPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetWorkerReport"))
              .build();
        }
      }
    }
    return getGetWorkerReportMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerLostStoragePOptions,
      alluxio.grpc.GetWorkerLostStoragePResponse> getGetWorkerLostStorageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetWorkerLostStorage",
      requestType = alluxio.grpc.GetWorkerLostStoragePOptions.class,
      responseType = alluxio.grpc.GetWorkerLostStoragePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerLostStoragePOptions,
      alluxio.grpc.GetWorkerLostStoragePResponse> getGetWorkerLostStorageMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetWorkerLostStoragePOptions, alluxio.grpc.GetWorkerLostStoragePResponse> getGetWorkerLostStorageMethod;
    if ((getGetWorkerLostStorageMethod = BlockMasterClientServiceGrpc.getGetWorkerLostStorageMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetWorkerLostStorageMethod = BlockMasterClientServiceGrpc.getGetWorkerLostStorageMethod) == null) {
          BlockMasterClientServiceGrpc.getGetWorkerLostStorageMethod = getGetWorkerLostStorageMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetWorkerLostStoragePOptions, alluxio.grpc.GetWorkerLostStoragePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetWorkerLostStorage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetWorkerLostStoragePOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetWorkerLostStoragePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetWorkerLostStorage"))
              .build();
        }
      }
    }
    return getGetWorkerLostStorageMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BlockMasterClientServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BlockMasterClientServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BlockMasterClientServiceStub>() {
        @java.lang.Override
        public BlockMasterClientServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BlockMasterClientServiceStub(channel, callOptions);
        }
      };
    return BlockMasterClientServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BlockMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BlockMasterClientServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BlockMasterClientServiceBlockingStub>() {
        @java.lang.Override
        public BlockMasterClientServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BlockMasterClientServiceBlockingStub(channel, callOptions);
        }
      };
    return BlockMasterClientServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BlockMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BlockMasterClientServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BlockMasterClientServiceFutureStub>() {
        @java.lang.Override
        public BlockMasterClientServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BlockMasterClientServiceFutureStub(channel, callOptions);
        }
      };
    return BlockMasterClientServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   **
   * This interface contains block master service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class BlockMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Returns the block information for the given block id.
     * </pre>
     */
    public void getBlockInfo(alluxio.grpc.GetBlockInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetBlockInfoPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetBlockInfoMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns block master information.
     * </pre>
     */
    public void getBlockMasterInfo(alluxio.grpc.GetBlockMasterInfoPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetBlockMasterInfoPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetBlockMasterInfoMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the capacity (in bytes).
     * </pre>
     */
    public void getCapacityBytes(alluxio.grpc.GetCapacityBytesPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetCapacityBytesPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCapacityBytesMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the used storage (in bytes).
     * </pre>
     */
    public void getUsedBytes(alluxio.grpc.GetUsedBytesPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetUsedBytesPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetUsedBytesMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information.
     * </pre>
     */
    public void getWorkerInfoList(alluxio.grpc.GetWorkerInfoListPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerInfoListPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetWorkerInfoListMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information for report CLI.
     * </pre>
     */
    public void getWorkerReport(alluxio.grpc.GetWorkerReportPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerInfoListPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetWorkerReportMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of worker lost storage information
     * </pre>
     */
    public void getWorkerLostStorage(alluxio.grpc.GetWorkerLostStoragePOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerLostStoragePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetWorkerLostStorageMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetBlockInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetBlockInfoPRequest,
                alluxio.grpc.GetBlockInfoPResponse>(
                  this, METHODID_GET_BLOCK_INFO)))
          .addMethod(
            getGetBlockMasterInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetBlockMasterInfoPOptions,
                alluxio.grpc.GetBlockMasterInfoPResponse>(
                  this, METHODID_GET_BLOCK_MASTER_INFO)))
          .addMethod(
            getGetCapacityBytesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetCapacityBytesPOptions,
                alluxio.grpc.GetCapacityBytesPResponse>(
                  this, METHODID_GET_CAPACITY_BYTES)))
          .addMethod(
            getGetUsedBytesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetUsedBytesPOptions,
                alluxio.grpc.GetUsedBytesPResponse>(
                  this, METHODID_GET_USED_BYTES)))
          .addMethod(
            getGetWorkerInfoListMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetWorkerInfoListPOptions,
                alluxio.grpc.GetWorkerInfoListPResponse>(
                  this, METHODID_GET_WORKER_INFO_LIST)))
          .addMethod(
            getGetWorkerReportMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetWorkerReportPOptions,
                alluxio.grpc.GetWorkerInfoListPResponse>(
                  this, METHODID_GET_WORKER_REPORT)))
          .addMethod(
            getGetWorkerLostStorageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetWorkerLostStoragePOptions,
                alluxio.grpc.GetWorkerLostStoragePResponse>(
                  this, METHODID_GET_WORKER_LOST_STORAGE)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains block master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class BlockMasterClientServiceStub extends io.grpc.stub.AbstractAsyncStub<BlockMasterClientServiceStub> {
    private BlockMasterClientServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockMasterClientServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BlockMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the block information for the given block id.
     * </pre>
     */
    public void getBlockInfo(alluxio.grpc.GetBlockInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetBlockInfoPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetBlockInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns block master information.
     * </pre>
     */
    public void getBlockMasterInfo(alluxio.grpc.GetBlockMasterInfoPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetBlockMasterInfoPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetBlockMasterInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the capacity (in bytes).
     * </pre>
     */
    public void getCapacityBytes(alluxio.grpc.GetCapacityBytesPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetCapacityBytesPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCapacityBytesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the used storage (in bytes).
     * </pre>
     */
    public void getUsedBytes(alluxio.grpc.GetUsedBytesPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetUsedBytesPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetUsedBytesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information.
     * </pre>
     */
    public void getWorkerInfoList(alluxio.grpc.GetWorkerInfoListPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerInfoListPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetWorkerInfoListMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information for report CLI.
     * </pre>
     */
    public void getWorkerReport(alluxio.grpc.GetWorkerReportPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerInfoListPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetWorkerReportMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of worker lost storage information
     * </pre>
     */
    public void getWorkerLostStorage(alluxio.grpc.GetWorkerLostStoragePOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerLostStoragePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetWorkerLostStorageMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains block master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class BlockMasterClientServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<BlockMasterClientServiceBlockingStub> {
    private BlockMasterClientServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockMasterClientServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BlockMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the block information for the given block id.
     * </pre>
     */
    public alluxio.grpc.GetBlockInfoPResponse getBlockInfo(alluxio.grpc.GetBlockInfoPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetBlockInfoMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns block master information.
     * </pre>
     */
    public alluxio.grpc.GetBlockMasterInfoPResponse getBlockMasterInfo(alluxio.grpc.GetBlockMasterInfoPOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetBlockMasterInfoMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the capacity (in bytes).
     * </pre>
     */
    public alluxio.grpc.GetCapacityBytesPResponse getCapacityBytes(alluxio.grpc.GetCapacityBytesPOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCapacityBytesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the used storage (in bytes).
     * </pre>
     */
    public alluxio.grpc.GetUsedBytesPResponse getUsedBytes(alluxio.grpc.GetUsedBytesPOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetUsedBytesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information.
     * </pre>
     */
    public alluxio.grpc.GetWorkerInfoListPResponse getWorkerInfoList(alluxio.grpc.GetWorkerInfoListPOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetWorkerInfoListMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information for report CLI.
     * </pre>
     */
    public alluxio.grpc.GetWorkerInfoListPResponse getWorkerReport(alluxio.grpc.GetWorkerReportPOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetWorkerReportMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a list of worker lost storage information
     * </pre>
     */
    public alluxio.grpc.GetWorkerLostStoragePResponse getWorkerLostStorage(alluxio.grpc.GetWorkerLostStoragePOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetWorkerLostStorageMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains block master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class BlockMasterClientServiceFutureStub extends io.grpc.stub.AbstractFutureStub<BlockMasterClientServiceFutureStub> {
    private BlockMasterClientServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockMasterClientServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BlockMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the block information for the given block id.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetBlockInfoPResponse> getBlockInfo(
        alluxio.grpc.GetBlockInfoPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetBlockInfoMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns block master information.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetBlockMasterInfoPResponse> getBlockMasterInfo(
        alluxio.grpc.GetBlockMasterInfoPOptions request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetBlockMasterInfoMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the capacity (in bytes).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetCapacityBytesPResponse> getCapacityBytes(
        alluxio.grpc.GetCapacityBytesPOptions request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCapacityBytesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the used storage (in bytes).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetUsedBytesPResponse> getUsedBytes(
        alluxio.grpc.GetUsedBytesPOptions request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetUsedBytesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetWorkerInfoListPResponse> getWorkerInfoList(
        alluxio.grpc.GetWorkerInfoListPOptions request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetWorkerInfoListMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information for report CLI.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetWorkerInfoListPResponse> getWorkerReport(
        alluxio.grpc.GetWorkerReportPOptions request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetWorkerReportMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a list of worker lost storage information
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetWorkerLostStoragePResponse> getWorkerLostStorage(
        alluxio.grpc.GetWorkerLostStoragePOptions request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetWorkerLostStorageMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_BLOCK_INFO = 0;
  private static final int METHODID_GET_BLOCK_MASTER_INFO = 1;
  private static final int METHODID_GET_CAPACITY_BYTES = 2;
  private static final int METHODID_GET_USED_BYTES = 3;
  private static final int METHODID_GET_WORKER_INFO_LIST = 4;
  private static final int METHODID_GET_WORKER_REPORT = 5;
  private static final int METHODID_GET_WORKER_LOST_STORAGE = 6;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BlockMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BlockMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_BLOCK_INFO:
          serviceImpl.getBlockInfo((alluxio.grpc.GetBlockInfoPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetBlockInfoPResponse>) responseObserver);
          break;
        case METHODID_GET_BLOCK_MASTER_INFO:
          serviceImpl.getBlockMasterInfo((alluxio.grpc.GetBlockMasterInfoPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetBlockMasterInfoPResponse>) responseObserver);
          break;
        case METHODID_GET_CAPACITY_BYTES:
          serviceImpl.getCapacityBytes((alluxio.grpc.GetCapacityBytesPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetCapacityBytesPResponse>) responseObserver);
          break;
        case METHODID_GET_USED_BYTES:
          serviceImpl.getUsedBytes((alluxio.grpc.GetUsedBytesPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetUsedBytesPResponse>) responseObserver);
          break;
        case METHODID_GET_WORKER_INFO_LIST:
          serviceImpl.getWorkerInfoList((alluxio.grpc.GetWorkerInfoListPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerInfoListPResponse>) responseObserver);
          break;
        case METHODID_GET_WORKER_REPORT:
          serviceImpl.getWorkerReport((alluxio.grpc.GetWorkerReportPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerInfoListPResponse>) responseObserver);
          break;
        case METHODID_GET_WORKER_LOST_STORAGE:
          serviceImpl.getWorkerLostStorage((alluxio.grpc.GetWorkerLostStoragePOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetWorkerLostStoragePResponse>) responseObserver);
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

  private static abstract class BlockMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BlockMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.BlockMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BlockMasterClientService");
    }
  }

  private static final class BlockMasterClientServiceFileDescriptorSupplier
      extends BlockMasterClientServiceBaseDescriptorSupplier {
    BlockMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class BlockMasterClientServiceMethodDescriptorSupplier
      extends BlockMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BlockMasterClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (BlockMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BlockMasterClientServiceFileDescriptorSupplier())
              .addMethod(getGetBlockInfoMethod())
              .addMethod(getGetBlockMasterInfoMethod())
              .addMethod(getGetCapacityBytesMethod())
              .addMethod(getGetUsedBytesMethod())
              .addMethod(getGetWorkerInfoListMethod())
              .addMethod(getGetWorkerReportMethod())
              .addMethod(getGetWorkerLostStorageMethod())
              .build();
        }
      }
    }
    return result;
  }
}
