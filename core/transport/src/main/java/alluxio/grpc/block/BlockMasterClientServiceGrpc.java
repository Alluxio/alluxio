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
 * This interface contains block master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.10.1)",
    comments = "Source: block_master.proto")
public final class BlockMasterClientServiceGrpc {

  private BlockMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.BlockMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetBlockInfoMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.block.GetBlockInfoPRequest,
      alluxio.grpc.block.GetBlockInfoPResponse> METHOD_GET_BLOCK_INFO = getGetBlockInfoMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.block.GetBlockInfoPRequest,
      alluxio.grpc.block.GetBlockInfoPResponse> getGetBlockInfoMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.block.GetBlockInfoPRequest,
      alluxio.grpc.block.GetBlockInfoPResponse> getGetBlockInfoMethod() {
    return getGetBlockInfoMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.block.GetBlockInfoPRequest,
      alluxio.grpc.block.GetBlockInfoPResponse> getGetBlockInfoMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.block.GetBlockInfoPRequest, alluxio.grpc.block.GetBlockInfoPResponse> getGetBlockInfoMethod;
    if ((getGetBlockInfoMethod = BlockMasterClientServiceGrpc.getGetBlockInfoMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetBlockInfoMethod = BlockMasterClientServiceGrpc.getGetBlockInfoMethod) == null) {
          BlockMasterClientServiceGrpc.getGetBlockInfoMethod = getGetBlockInfoMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.block.GetBlockInfoPRequest, alluxio.grpc.block.GetBlockInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockMasterClientService", "GetBlockInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetBlockInfoPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetBlockInfoPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetBlockInfo"))
                  .build();
          }
        }
     }
     return getGetBlockInfoMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetBlockMasterInfoMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.block.GetBlockMasterInfoPOptions,
      alluxio.grpc.block.GetBlockMasterInfoPResponse> METHOD_GET_BLOCK_MASTER_INFO = getGetBlockMasterInfoMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.block.GetBlockMasterInfoPOptions,
      alluxio.grpc.block.GetBlockMasterInfoPResponse> getGetBlockMasterInfoMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.block.GetBlockMasterInfoPOptions,
      alluxio.grpc.block.GetBlockMasterInfoPResponse> getGetBlockMasterInfoMethod() {
    return getGetBlockMasterInfoMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.block.GetBlockMasterInfoPOptions,
      alluxio.grpc.block.GetBlockMasterInfoPResponse> getGetBlockMasterInfoMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.block.GetBlockMasterInfoPOptions, alluxio.grpc.block.GetBlockMasterInfoPResponse> getGetBlockMasterInfoMethod;
    if ((getGetBlockMasterInfoMethod = BlockMasterClientServiceGrpc.getGetBlockMasterInfoMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetBlockMasterInfoMethod = BlockMasterClientServiceGrpc.getGetBlockMasterInfoMethod) == null) {
          BlockMasterClientServiceGrpc.getGetBlockMasterInfoMethod = getGetBlockMasterInfoMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.block.GetBlockMasterInfoPOptions, alluxio.grpc.block.GetBlockMasterInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockMasterClientService", "GetBlockMasterInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetBlockMasterInfoPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetBlockMasterInfoPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetBlockMasterInfo"))
                  .build();
          }
        }
     }
     return getGetBlockMasterInfoMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetCapacityBytesMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.block.GetCapacityBytesPOptions,
      alluxio.grpc.block.GetCapacityBytesPResponse> METHOD_GET_CAPACITY_BYTES = getGetCapacityBytesMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.block.GetCapacityBytesPOptions,
      alluxio.grpc.block.GetCapacityBytesPResponse> getGetCapacityBytesMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.block.GetCapacityBytesPOptions,
      alluxio.grpc.block.GetCapacityBytesPResponse> getGetCapacityBytesMethod() {
    return getGetCapacityBytesMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.block.GetCapacityBytesPOptions,
      alluxio.grpc.block.GetCapacityBytesPResponse> getGetCapacityBytesMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.block.GetCapacityBytesPOptions, alluxio.grpc.block.GetCapacityBytesPResponse> getGetCapacityBytesMethod;
    if ((getGetCapacityBytesMethod = BlockMasterClientServiceGrpc.getGetCapacityBytesMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetCapacityBytesMethod = BlockMasterClientServiceGrpc.getGetCapacityBytesMethod) == null) {
          BlockMasterClientServiceGrpc.getGetCapacityBytesMethod = getGetCapacityBytesMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.block.GetCapacityBytesPOptions, alluxio.grpc.block.GetCapacityBytesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockMasterClientService", "GetCapacityBytes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetCapacityBytesPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetCapacityBytesPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetCapacityBytes"))
                  .build();
          }
        }
     }
     return getGetCapacityBytesMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetUsedBytesMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.block.GetUsedBytesPOptions,
      alluxio.grpc.block.GetUsedBytesPResponse> METHOD_GET_USED_BYTES = getGetUsedBytesMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.block.GetUsedBytesPOptions,
      alluxio.grpc.block.GetUsedBytesPResponse> getGetUsedBytesMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.block.GetUsedBytesPOptions,
      alluxio.grpc.block.GetUsedBytesPResponse> getGetUsedBytesMethod() {
    return getGetUsedBytesMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.block.GetUsedBytesPOptions,
      alluxio.grpc.block.GetUsedBytesPResponse> getGetUsedBytesMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.block.GetUsedBytesPOptions, alluxio.grpc.block.GetUsedBytesPResponse> getGetUsedBytesMethod;
    if ((getGetUsedBytesMethod = BlockMasterClientServiceGrpc.getGetUsedBytesMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetUsedBytesMethod = BlockMasterClientServiceGrpc.getGetUsedBytesMethod) == null) {
          BlockMasterClientServiceGrpc.getGetUsedBytesMethod = getGetUsedBytesMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.block.GetUsedBytesPOptions, alluxio.grpc.block.GetUsedBytesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockMasterClientService", "GetUsedBytes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetUsedBytesPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetUsedBytesPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetUsedBytes"))
                  .build();
          }
        }
     }
     return getGetUsedBytesMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetWorkerInfoListMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerInfoListPOptions,
      alluxio.grpc.block.GetWorkerInfoListPResponse> METHOD_GET_WORKER_INFO_LIST = getGetWorkerInfoListMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerInfoListPOptions,
      alluxio.grpc.block.GetWorkerInfoListPResponse> getGetWorkerInfoListMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerInfoListPOptions,
      alluxio.grpc.block.GetWorkerInfoListPResponse> getGetWorkerInfoListMethod() {
    return getGetWorkerInfoListMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerInfoListPOptions,
      alluxio.grpc.block.GetWorkerInfoListPResponse> getGetWorkerInfoListMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerInfoListPOptions, alluxio.grpc.block.GetWorkerInfoListPResponse> getGetWorkerInfoListMethod;
    if ((getGetWorkerInfoListMethod = BlockMasterClientServiceGrpc.getGetWorkerInfoListMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetWorkerInfoListMethod = BlockMasterClientServiceGrpc.getGetWorkerInfoListMethod) == null) {
          BlockMasterClientServiceGrpc.getGetWorkerInfoListMethod = getGetWorkerInfoListMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.block.GetWorkerInfoListPOptions, alluxio.grpc.block.GetWorkerInfoListPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockMasterClientService", "GetWorkerInfoList"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetWorkerInfoListPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetWorkerInfoListPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetWorkerInfoList"))
                  .build();
          }
        }
     }
     return getGetWorkerInfoListMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetWorkerReportMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerReportPOptions,
      alluxio.grpc.block.GetWorkerInfoListPResponse> METHOD_GET_WORKER_REPORT = getGetWorkerReportMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerReportPOptions,
      alluxio.grpc.block.GetWorkerInfoListPResponse> getGetWorkerReportMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerReportPOptions,
      alluxio.grpc.block.GetWorkerInfoListPResponse> getGetWorkerReportMethod() {
    return getGetWorkerReportMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerReportPOptions,
      alluxio.grpc.block.GetWorkerInfoListPResponse> getGetWorkerReportMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.block.GetWorkerReportPOptions, alluxio.grpc.block.GetWorkerInfoListPResponse> getGetWorkerReportMethod;
    if ((getGetWorkerReportMethod = BlockMasterClientServiceGrpc.getGetWorkerReportMethod) == null) {
      synchronized (BlockMasterClientServiceGrpc.class) {
        if ((getGetWorkerReportMethod = BlockMasterClientServiceGrpc.getGetWorkerReportMethod) == null) {
          BlockMasterClientServiceGrpc.getGetWorkerReportMethod = getGetWorkerReportMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.block.GetWorkerReportPOptions, alluxio.grpc.block.GetWorkerInfoListPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockMasterClientService", "GetWorkerReport"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetWorkerReportPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.block.GetWorkerInfoListPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockMasterClientServiceMethodDescriptorSupplier("GetWorkerReport"))
                  .build();
          }
        }
     }
     return getGetWorkerReportMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BlockMasterClientServiceStub newStub(io.grpc.Channel channel) {
    return new BlockMasterClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BlockMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BlockMasterClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BlockMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BlockMasterClientServiceFutureStub(channel);
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
    public void getBlockInfo(alluxio.grpc.block.GetBlockInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetBlockInfoPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetBlockInfoMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns block master information.
     * </pre>
     */
    public void getBlockMasterInfo(alluxio.grpc.block.GetBlockMasterInfoPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetBlockMasterInfoPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetBlockMasterInfoMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the capacity (in bytes).
     * </pre>
     */
    public void getCapacityBytes(alluxio.grpc.block.GetCapacityBytesPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetCapacityBytesPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetCapacityBytesMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the used storage (in bytes).
     * </pre>
     */
    public void getUsedBytes(alluxio.grpc.block.GetUsedBytesPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetUsedBytesPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetUsedBytesMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information.
     * </pre>
     */
    public void getWorkerInfoList(alluxio.grpc.block.GetWorkerInfoListPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetWorkerInfoListPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetWorkerInfoListMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information for report CLI.
     * </pre>
     */
    public void getWorkerReport(alluxio.grpc.block.GetWorkerReportPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetWorkerInfoListPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetWorkerReportMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetBlockInfoMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.block.GetBlockInfoPRequest,
                alluxio.grpc.block.GetBlockInfoPResponse>(
                  this, METHODID_GET_BLOCK_INFO)))
          .addMethod(
            getGetBlockMasterInfoMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.block.GetBlockMasterInfoPOptions,
                alluxio.grpc.block.GetBlockMasterInfoPResponse>(
                  this, METHODID_GET_BLOCK_MASTER_INFO)))
          .addMethod(
            getGetCapacityBytesMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.block.GetCapacityBytesPOptions,
                alluxio.grpc.block.GetCapacityBytesPResponse>(
                  this, METHODID_GET_CAPACITY_BYTES)))
          .addMethod(
            getGetUsedBytesMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.block.GetUsedBytesPOptions,
                alluxio.grpc.block.GetUsedBytesPResponse>(
                  this, METHODID_GET_USED_BYTES)))
          .addMethod(
            getGetWorkerInfoListMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.block.GetWorkerInfoListPOptions,
                alluxio.grpc.block.GetWorkerInfoListPResponse>(
                  this, METHODID_GET_WORKER_INFO_LIST)))
          .addMethod(
            getGetWorkerReportMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.block.GetWorkerReportPOptions,
                alluxio.grpc.block.GetWorkerInfoListPResponse>(
                  this, METHODID_GET_WORKER_REPORT)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains block master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class BlockMasterClientServiceStub extends io.grpc.stub.AbstractStub<BlockMasterClientServiceStub> {
    private BlockMasterClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockMasterClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockMasterClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the block information for the given block id.
     * </pre>
     */
    public void getBlockInfo(alluxio.grpc.block.GetBlockInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetBlockInfoPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetBlockInfoMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns block master information.
     * </pre>
     */
    public void getBlockMasterInfo(alluxio.grpc.block.GetBlockMasterInfoPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetBlockMasterInfoPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetBlockMasterInfoMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the capacity (in bytes).
     * </pre>
     */
    public void getCapacityBytes(alluxio.grpc.block.GetCapacityBytesPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetCapacityBytesPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetCapacityBytesMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the used storage (in bytes).
     * </pre>
     */
    public void getUsedBytes(alluxio.grpc.block.GetUsedBytesPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetUsedBytesPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetUsedBytesMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information.
     * </pre>
     */
    public void getWorkerInfoList(alluxio.grpc.block.GetWorkerInfoListPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetWorkerInfoListPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetWorkerInfoListMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information for report CLI.
     * </pre>
     */
    public void getWorkerReport(alluxio.grpc.block.GetWorkerReportPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.block.GetWorkerInfoListPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetWorkerReportMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains block master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class BlockMasterClientServiceBlockingStub extends io.grpc.stub.AbstractStub<BlockMasterClientServiceBlockingStub> {
    private BlockMasterClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockMasterClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockMasterClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the block information for the given block id.
     * </pre>
     */
    public alluxio.grpc.block.GetBlockInfoPResponse getBlockInfo(alluxio.grpc.block.GetBlockInfoPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetBlockInfoMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns block master information.
     * </pre>
     */
    public alluxio.grpc.block.GetBlockMasterInfoPResponse getBlockMasterInfo(alluxio.grpc.block.GetBlockMasterInfoPOptions request) {
      return blockingUnaryCall(
          getChannel(), getGetBlockMasterInfoMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the capacity (in bytes).
     * </pre>
     */
    public alluxio.grpc.block.GetCapacityBytesPResponse getCapacityBytes(alluxio.grpc.block.GetCapacityBytesPOptions request) {
      return blockingUnaryCall(
          getChannel(), getGetCapacityBytesMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the used storage (in bytes).
     * </pre>
     */
    public alluxio.grpc.block.GetUsedBytesPResponse getUsedBytes(alluxio.grpc.block.GetUsedBytesPOptions request) {
      return blockingUnaryCall(
          getChannel(), getGetUsedBytesMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information.
     * </pre>
     */
    public alluxio.grpc.block.GetWorkerInfoListPResponse getWorkerInfoList(alluxio.grpc.block.GetWorkerInfoListPOptions request) {
      return blockingUnaryCall(
          getChannel(), getGetWorkerInfoListMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information for report CLI.
     * </pre>
     */
    public alluxio.grpc.block.GetWorkerInfoListPResponse getWorkerReport(alluxio.grpc.block.GetWorkerReportPOptions request) {
      return blockingUnaryCall(
          getChannel(), getGetWorkerReportMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains block master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class BlockMasterClientServiceFutureStub extends io.grpc.stub.AbstractStub<BlockMasterClientServiceFutureStub> {
    private BlockMasterClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockMasterClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockMasterClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns the block information for the given block id.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.block.GetBlockInfoPResponse> getBlockInfo(
        alluxio.grpc.block.GetBlockInfoPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetBlockInfoMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns block master information.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.block.GetBlockMasterInfoPResponse> getBlockMasterInfo(
        alluxio.grpc.block.GetBlockMasterInfoPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getGetBlockMasterInfoMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the capacity (in bytes).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.block.GetCapacityBytesPResponse> getCapacityBytes(
        alluxio.grpc.block.GetCapacityBytesPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getGetCapacityBytesMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the used storage (in bytes).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.block.GetUsedBytesPResponse> getUsedBytes(
        alluxio.grpc.block.GetUsedBytesPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getGetUsedBytesMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.block.GetWorkerInfoListPResponse> getWorkerInfoList(
        alluxio.grpc.block.GetWorkerInfoListPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getGetWorkerInfoListMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a list of workers information for report CLI.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.block.GetWorkerInfoListPResponse> getWorkerReport(
        alluxio.grpc.block.GetWorkerReportPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getGetWorkerReportMethodHelper(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_BLOCK_INFO = 0;
  private static final int METHODID_GET_BLOCK_MASTER_INFO = 1;
  private static final int METHODID_GET_CAPACITY_BYTES = 2;
  private static final int METHODID_GET_USED_BYTES = 3;
  private static final int METHODID_GET_WORKER_INFO_LIST = 4;
  private static final int METHODID_GET_WORKER_REPORT = 5;

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
          serviceImpl.getBlockInfo((alluxio.grpc.block.GetBlockInfoPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.block.GetBlockInfoPResponse>) responseObserver);
          break;
        case METHODID_GET_BLOCK_MASTER_INFO:
          serviceImpl.getBlockMasterInfo((alluxio.grpc.block.GetBlockMasterInfoPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.block.GetBlockMasterInfoPResponse>) responseObserver);
          break;
        case METHODID_GET_CAPACITY_BYTES:
          serviceImpl.getCapacityBytes((alluxio.grpc.block.GetCapacityBytesPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.block.GetCapacityBytesPResponse>) responseObserver);
          break;
        case METHODID_GET_USED_BYTES:
          serviceImpl.getUsedBytes((alluxio.grpc.block.GetUsedBytesPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.block.GetUsedBytesPResponse>) responseObserver);
          break;
        case METHODID_GET_WORKER_INFO_LIST:
          serviceImpl.getWorkerInfoList((alluxio.grpc.block.GetWorkerInfoListPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.block.GetWorkerInfoListPResponse>) responseObserver);
          break;
        case METHODID_GET_WORKER_REPORT:
          serviceImpl.getWorkerReport((alluxio.grpc.block.GetWorkerReportPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.block.GetWorkerInfoListPResponse>) responseObserver);
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
      return alluxio.grpc.block.BlockMasterProto.getDescriptor();
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
              .addMethod(getGetBlockInfoMethodHelper())
              .addMethod(getGetBlockMasterInfoMethodHelper())
              .addMethod(getGetCapacityBytesMethodHelper())
              .addMethod(getGetUsedBytesMethodHelper())
              .addMethod(getGetWorkerInfoListMethodHelper())
              .addMethod(getGetWorkerReportMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
