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
 * This interface contains key-value master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: grpc/key_value_master.proto")
public final class KeyValueMasterClientServiceGrpc {

  private KeyValueMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.keyvalue.KeyValueMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CompletePartitionPRequest,
      alluxio.grpc.CompletePartitionPResponse> getCompletePartitionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CompletePartition",
      requestType = alluxio.grpc.CompletePartitionPRequest.class,
      responseType = alluxio.grpc.CompletePartitionPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CompletePartitionPRequest,
      alluxio.grpc.CompletePartitionPResponse> getCompletePartitionMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CompletePartitionPRequest, alluxio.grpc.CompletePartitionPResponse> getCompletePartitionMethod;
    if ((getCompletePartitionMethod = KeyValueMasterClientServiceGrpc.getCompletePartitionMethod) == null) {
      synchronized (KeyValueMasterClientServiceGrpc.class) {
        if ((getCompletePartitionMethod = KeyValueMasterClientServiceGrpc.getCompletePartitionMethod) == null) {
          KeyValueMasterClientServiceGrpc.getCompletePartitionMethod = getCompletePartitionMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CompletePartitionPRequest, alluxio.grpc.CompletePartitionPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.keyvalue.KeyValueMasterClientService", "CompletePartition"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CompletePartitionPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CompletePartitionPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new KeyValueMasterClientServiceMethodDescriptorSupplier("CompletePartition"))
                  .build();
          }
        }
     }
     return getCompletePartitionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CompleteStorePRequest,
      alluxio.grpc.CompleteStorePResponse> getCompleteStoreMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "completeStore",
      requestType = alluxio.grpc.CompleteStorePRequest.class,
      responseType = alluxio.grpc.CompleteStorePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CompleteStorePRequest,
      alluxio.grpc.CompleteStorePResponse> getCompleteStoreMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CompleteStorePRequest, alluxio.grpc.CompleteStorePResponse> getCompleteStoreMethod;
    if ((getCompleteStoreMethod = KeyValueMasterClientServiceGrpc.getCompleteStoreMethod) == null) {
      synchronized (KeyValueMasterClientServiceGrpc.class) {
        if ((getCompleteStoreMethod = KeyValueMasterClientServiceGrpc.getCompleteStoreMethod) == null) {
          KeyValueMasterClientServiceGrpc.getCompleteStoreMethod = getCompleteStoreMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CompleteStorePRequest, alluxio.grpc.CompleteStorePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.keyvalue.KeyValueMasterClientService", "completeStore"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CompleteStorePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CompleteStorePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new KeyValueMasterClientServiceMethodDescriptorSupplier("completeStore"))
                  .build();
          }
        }
     }
     return getCompleteStoreMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CreateStorePRequest,
      alluxio.grpc.CreateStorePResponse> getCreateStoreMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateStore",
      requestType = alluxio.grpc.CreateStorePRequest.class,
      responseType = alluxio.grpc.CreateStorePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CreateStorePRequest,
      alluxio.grpc.CreateStorePResponse> getCreateStoreMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CreateStorePRequest, alluxio.grpc.CreateStorePResponse> getCreateStoreMethod;
    if ((getCreateStoreMethod = KeyValueMasterClientServiceGrpc.getCreateStoreMethod) == null) {
      synchronized (KeyValueMasterClientServiceGrpc.class) {
        if ((getCreateStoreMethod = KeyValueMasterClientServiceGrpc.getCreateStoreMethod) == null) {
          KeyValueMasterClientServiceGrpc.getCreateStoreMethod = getCreateStoreMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CreateStorePRequest, alluxio.grpc.CreateStorePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.keyvalue.KeyValueMasterClientService", "CreateStore"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateStorePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateStorePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new KeyValueMasterClientServiceMethodDescriptorSupplier("CreateStore"))
                  .build();
          }
        }
     }
     return getCreateStoreMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.DeleteStorePRequest,
      alluxio.grpc.DeleteStorePResponse> getDeleteStoreMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteStore",
      requestType = alluxio.grpc.DeleteStorePRequest.class,
      responseType = alluxio.grpc.DeleteStorePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.DeleteStorePRequest,
      alluxio.grpc.DeleteStorePResponse> getDeleteStoreMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.DeleteStorePRequest, alluxio.grpc.DeleteStorePResponse> getDeleteStoreMethod;
    if ((getDeleteStoreMethod = KeyValueMasterClientServiceGrpc.getDeleteStoreMethod) == null) {
      synchronized (KeyValueMasterClientServiceGrpc.class) {
        if ((getDeleteStoreMethod = KeyValueMasterClientServiceGrpc.getDeleteStoreMethod) == null) {
          KeyValueMasterClientServiceGrpc.getDeleteStoreMethod = getDeleteStoreMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.DeleteStorePRequest, alluxio.grpc.DeleteStorePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.keyvalue.KeyValueMasterClientService", "DeleteStore"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.DeleteStorePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.DeleteStorePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new KeyValueMasterClientServiceMethodDescriptorSupplier("DeleteStore"))
                  .build();
          }
        }
     }
     return getDeleteStoreMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetPartitionInfoPRequest,
      alluxio.grpc.GetPartitionInfoPResponse> getGetPartitionInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPartitionInfo",
      requestType = alluxio.grpc.GetPartitionInfoPRequest.class,
      responseType = alluxio.grpc.GetPartitionInfoPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetPartitionInfoPRequest,
      alluxio.grpc.GetPartitionInfoPResponse> getGetPartitionInfoMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetPartitionInfoPRequest, alluxio.grpc.GetPartitionInfoPResponse> getGetPartitionInfoMethod;
    if ((getGetPartitionInfoMethod = KeyValueMasterClientServiceGrpc.getGetPartitionInfoMethod) == null) {
      synchronized (KeyValueMasterClientServiceGrpc.class) {
        if ((getGetPartitionInfoMethod = KeyValueMasterClientServiceGrpc.getGetPartitionInfoMethod) == null) {
          KeyValueMasterClientServiceGrpc.getGetPartitionInfoMethod = getGetPartitionInfoMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetPartitionInfoPRequest, alluxio.grpc.GetPartitionInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.keyvalue.KeyValueMasterClientService", "GetPartitionInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetPartitionInfoPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetPartitionInfoPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new KeyValueMasterClientServiceMethodDescriptorSupplier("GetPartitionInfo"))
                  .build();
          }
        }
     }
     return getGetPartitionInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.MergeStorePRequest,
      alluxio.grpc.MergeStorePResponse> getMergeStoreMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MergeStore",
      requestType = alluxio.grpc.MergeStorePRequest.class,
      responseType = alluxio.grpc.MergeStorePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.MergeStorePRequest,
      alluxio.grpc.MergeStorePResponse> getMergeStoreMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.MergeStorePRequest, alluxio.grpc.MergeStorePResponse> getMergeStoreMethod;
    if ((getMergeStoreMethod = KeyValueMasterClientServiceGrpc.getMergeStoreMethod) == null) {
      synchronized (KeyValueMasterClientServiceGrpc.class) {
        if ((getMergeStoreMethod = KeyValueMasterClientServiceGrpc.getMergeStoreMethod) == null) {
          KeyValueMasterClientServiceGrpc.getMergeStoreMethod = getMergeStoreMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.MergeStorePRequest, alluxio.grpc.MergeStorePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.keyvalue.KeyValueMasterClientService", "MergeStore"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.MergeStorePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.MergeStorePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new KeyValueMasterClientServiceMethodDescriptorSupplier("MergeStore"))
                  .build();
          }
        }
     }
     return getMergeStoreMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RenameStorePRequest,
      alluxio.grpc.RenameStorePResponse> getRenameStoreMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RenameStore",
      requestType = alluxio.grpc.RenameStorePRequest.class,
      responseType = alluxio.grpc.RenameStorePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.RenameStorePRequest,
      alluxio.grpc.RenameStorePResponse> getRenameStoreMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.RenameStorePRequest, alluxio.grpc.RenameStorePResponse> getRenameStoreMethod;
    if ((getRenameStoreMethod = KeyValueMasterClientServiceGrpc.getRenameStoreMethod) == null) {
      synchronized (KeyValueMasterClientServiceGrpc.class) {
        if ((getRenameStoreMethod = KeyValueMasterClientServiceGrpc.getRenameStoreMethod) == null) {
          KeyValueMasterClientServiceGrpc.getRenameStoreMethod = getRenameStoreMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.RenameStorePRequest, alluxio.grpc.RenameStorePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.keyvalue.KeyValueMasterClientService", "RenameStore"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RenameStorePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RenameStorePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new KeyValueMasterClientServiceMethodDescriptorSupplier("RenameStore"))
                  .build();
          }
        }
     }
     return getRenameStoreMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static KeyValueMasterClientServiceStub newStub(io.grpc.Channel channel) {
    return new KeyValueMasterClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static KeyValueMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new KeyValueMasterClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static KeyValueMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new KeyValueMasterClientServiceFutureStub(channel);
  }

  /**
   * <pre>
   **
   * This interface contains key-value master service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class KeyValueMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Marks a partition complete and adds it to the store.
     * </pre>
     */
    public void completePartition(alluxio.grpc.CompletePartitionPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CompletePartitionPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCompletePartitionMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Marks a store complete with its filesystem path.
     * </pre>
     */
    public void completeStore(alluxio.grpc.CompleteStorePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CompleteStorePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCompleteStoreMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a new key-value store on master.
     * </pre>
     */
    public void createStore(alluxio.grpc.CreateStorePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateStorePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateStoreMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Deletes a completed key-value store.
     * </pre>
     */
    public void deleteStore(alluxio.grpc.DeleteStorePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.DeleteStorePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getDeleteStoreMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Gets the partition information for the key-value store at the given filesystem path.
     * </pre>
     */
    public void getPartitionInfo(alluxio.grpc.GetPartitionInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetPartitionInfoPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetPartitionInfoMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Merges one completed key-value store to another completed key-value store.
     * </pre>
     */
    public void mergeStore(alluxio.grpc.MergeStorePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MergeStorePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getMergeStoreMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Renames a completed key-value store.
     * </pre>
     */
    public void renameStore(alluxio.grpc.RenameStorePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RenameStorePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRenameStoreMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCompletePartitionMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CompletePartitionPRequest,
                alluxio.grpc.CompletePartitionPResponse>(
                  this, METHODID_COMPLETE_PARTITION)))
          .addMethod(
            getCompleteStoreMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CompleteStorePRequest,
                alluxio.grpc.CompleteStorePResponse>(
                  this, METHODID_COMPLETE_STORE)))
          .addMethod(
            getCreateStoreMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CreateStorePRequest,
                alluxio.grpc.CreateStorePResponse>(
                  this, METHODID_CREATE_STORE)))
          .addMethod(
            getDeleteStoreMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.DeleteStorePRequest,
                alluxio.grpc.DeleteStorePResponse>(
                  this, METHODID_DELETE_STORE)))
          .addMethod(
            getGetPartitionInfoMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetPartitionInfoPRequest,
                alluxio.grpc.GetPartitionInfoPResponse>(
                  this, METHODID_GET_PARTITION_INFO)))
          .addMethod(
            getMergeStoreMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.MergeStorePRequest,
                alluxio.grpc.MergeStorePResponse>(
                  this, METHODID_MERGE_STORE)))
          .addMethod(
            getRenameStoreMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RenameStorePRequest,
                alluxio.grpc.RenameStorePResponse>(
                  this, METHODID_RENAME_STORE)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains key-value master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class KeyValueMasterClientServiceStub extends io.grpc.stub.AbstractStub<KeyValueMasterClientServiceStub> {
    private KeyValueMasterClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private KeyValueMasterClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KeyValueMasterClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new KeyValueMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Marks a partition complete and adds it to the store.
     * </pre>
     */
    public void completePartition(alluxio.grpc.CompletePartitionPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CompletePartitionPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCompletePartitionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Marks a store complete with its filesystem path.
     * </pre>
     */
    public void completeStore(alluxio.grpc.CompleteStorePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CompleteStorePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCompleteStoreMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a new key-value store on master.
     * </pre>
     */
    public void createStore(alluxio.grpc.CreateStorePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateStorePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateStoreMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Deletes a completed key-value store.
     * </pre>
     */
    public void deleteStore(alluxio.grpc.DeleteStorePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.DeleteStorePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDeleteStoreMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Gets the partition information for the key-value store at the given filesystem path.
     * </pre>
     */
    public void getPartitionInfo(alluxio.grpc.GetPartitionInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetPartitionInfoPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetPartitionInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Merges one completed key-value store to another completed key-value store.
     * </pre>
     */
    public void mergeStore(alluxio.grpc.MergeStorePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MergeStorePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getMergeStoreMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Renames a completed key-value store.
     * </pre>
     */
    public void renameStore(alluxio.grpc.RenameStorePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RenameStorePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRenameStoreMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains key-value master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class KeyValueMasterClientServiceBlockingStub extends io.grpc.stub.AbstractStub<KeyValueMasterClientServiceBlockingStub> {
    private KeyValueMasterClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private KeyValueMasterClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KeyValueMasterClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new KeyValueMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Marks a partition complete and adds it to the store.
     * </pre>
     */
    public alluxio.grpc.CompletePartitionPResponse completePartition(alluxio.grpc.CompletePartitionPRequest request) {
      return blockingUnaryCall(
          getChannel(), getCompletePartitionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Marks a store complete with its filesystem path.
     * </pre>
     */
    public alluxio.grpc.CompleteStorePResponse completeStore(alluxio.grpc.CompleteStorePRequest request) {
      return blockingUnaryCall(
          getChannel(), getCompleteStoreMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Creates a new key-value store on master.
     * </pre>
     */
    public alluxio.grpc.CreateStorePResponse createStore(alluxio.grpc.CreateStorePRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateStoreMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Deletes a completed key-value store.
     * </pre>
     */
    public alluxio.grpc.DeleteStorePResponse deleteStore(alluxio.grpc.DeleteStorePRequest request) {
      return blockingUnaryCall(
          getChannel(), getDeleteStoreMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Gets the partition information for the key-value store at the given filesystem path.
     * </pre>
     */
    public alluxio.grpc.GetPartitionInfoPResponse getPartitionInfo(alluxio.grpc.GetPartitionInfoPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetPartitionInfoMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Merges one completed key-value store to another completed key-value store.
     * </pre>
     */
    public alluxio.grpc.MergeStorePResponse mergeStore(alluxio.grpc.MergeStorePRequest request) {
      return blockingUnaryCall(
          getChannel(), getMergeStoreMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Renames a completed key-value store.
     * </pre>
     */
    public alluxio.grpc.RenameStorePResponse renameStore(alluxio.grpc.RenameStorePRequest request) {
      return blockingUnaryCall(
          getChannel(), getRenameStoreMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains key-value master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class KeyValueMasterClientServiceFutureStub extends io.grpc.stub.AbstractStub<KeyValueMasterClientServiceFutureStub> {
    private KeyValueMasterClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private KeyValueMasterClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected KeyValueMasterClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new KeyValueMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Marks a partition complete and adds it to the store.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CompletePartitionPResponse> completePartition(
        alluxio.grpc.CompletePartitionPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCompletePartitionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Marks a store complete with its filesystem path.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CompleteStorePResponse> completeStore(
        alluxio.grpc.CompleteStorePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCompleteStoreMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Creates a new key-value store on master.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CreateStorePResponse> createStore(
        alluxio.grpc.CreateStorePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateStoreMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Deletes a completed key-value store.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.DeleteStorePResponse> deleteStore(
        alluxio.grpc.DeleteStorePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDeleteStoreMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Gets the partition information for the key-value store at the given filesystem path.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetPartitionInfoPResponse> getPartitionInfo(
        alluxio.grpc.GetPartitionInfoPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetPartitionInfoMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Merges one completed key-value store to another completed key-value store.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.MergeStorePResponse> mergeStore(
        alluxio.grpc.MergeStorePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getMergeStoreMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Renames a completed key-value store.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RenameStorePResponse> renameStore(
        alluxio.grpc.RenameStorePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRenameStoreMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_COMPLETE_PARTITION = 0;
  private static final int METHODID_COMPLETE_STORE = 1;
  private static final int METHODID_CREATE_STORE = 2;
  private static final int METHODID_DELETE_STORE = 3;
  private static final int METHODID_GET_PARTITION_INFO = 4;
  private static final int METHODID_MERGE_STORE = 5;
  private static final int METHODID_RENAME_STORE = 6;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final KeyValueMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(KeyValueMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_COMPLETE_PARTITION:
          serviceImpl.completePartition((alluxio.grpc.CompletePartitionPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CompletePartitionPResponse>) responseObserver);
          break;
        case METHODID_COMPLETE_STORE:
          serviceImpl.completeStore((alluxio.grpc.CompleteStorePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CompleteStorePResponse>) responseObserver);
          break;
        case METHODID_CREATE_STORE:
          serviceImpl.createStore((alluxio.grpc.CreateStorePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CreateStorePResponse>) responseObserver);
          break;
        case METHODID_DELETE_STORE:
          serviceImpl.deleteStore((alluxio.grpc.DeleteStorePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.DeleteStorePResponse>) responseObserver);
          break;
        case METHODID_GET_PARTITION_INFO:
          serviceImpl.getPartitionInfo((alluxio.grpc.GetPartitionInfoPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetPartitionInfoPResponse>) responseObserver);
          break;
        case METHODID_MERGE_STORE:
          serviceImpl.mergeStore((alluxio.grpc.MergeStorePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.MergeStorePResponse>) responseObserver);
          break;
        case METHODID_RENAME_STORE:
          serviceImpl.renameStore((alluxio.grpc.RenameStorePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RenameStorePResponse>) responseObserver);
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

  private static abstract class KeyValueMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    KeyValueMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.KeyValueMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("KeyValueMasterClientService");
    }
  }

  private static final class KeyValueMasterClientServiceFileDescriptorSupplier
      extends KeyValueMasterClientServiceBaseDescriptorSupplier {
    KeyValueMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class KeyValueMasterClientServiceMethodDescriptorSupplier
      extends KeyValueMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    KeyValueMasterClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (KeyValueMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new KeyValueMasterClientServiceFileDescriptorSupplier())
              .addMethod(getCompletePartitionMethod())
              .addMethod(getCompleteStoreMethod())
              .addMethod(getCreateStoreMethod())
              .addMethod(getDeleteStoreMethod())
              .addMethod(getGetPartitionInfoMethod())
              .addMethod(getMergeStoreMethod())
              .addMethod(getRenameStoreMethod())
              .build();
        }
      }
    }
    return result;
  }
}
