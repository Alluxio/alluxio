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
 * This interface contains file system worker service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.10.1)",
    comments = "Source: file_system_worker.proto")
public final class FileSystemWorkerClientServiceGrpc {

  private FileSystemWorkerClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.FileSystemWorkerClientService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getCancelUfsFileMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.CancelUfsFilePRequest,
      alluxio.grpc.CancelUfsFilePResponse> METHOD_CANCEL_UFS_FILE = getCancelUfsFileMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CancelUfsFilePRequest,
      alluxio.grpc.CancelUfsFilePResponse> getCancelUfsFileMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.CancelUfsFilePRequest,
      alluxio.grpc.CancelUfsFilePResponse> getCancelUfsFileMethod() {
    return getCancelUfsFileMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.CancelUfsFilePRequest,
      alluxio.grpc.CancelUfsFilePResponse> getCancelUfsFileMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.CancelUfsFilePRequest, alluxio.grpc.CancelUfsFilePResponse> getCancelUfsFileMethod;
    if ((getCancelUfsFileMethod = FileSystemWorkerClientServiceGrpc.getCancelUfsFileMethod) == null) {
      synchronized (FileSystemWorkerClientServiceGrpc.class) {
        if ((getCancelUfsFileMethod = FileSystemWorkerClientServiceGrpc.getCancelUfsFileMethod) == null) {
          FileSystemWorkerClientServiceGrpc.getCancelUfsFileMethod = getCancelUfsFileMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CancelUfsFilePRequest, alluxio.grpc.CancelUfsFilePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.FileSystemWorkerClientService", "CancelUfsFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CancelUfsFilePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CancelUfsFilePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemWorkerClientServiceMethodDescriptorSupplier("CancelUfsFile"))
                  .build();
          }
        }
     }
     return getCancelUfsFileMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getCloseUfsFileMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.CloseUfsFilePRequest,
      alluxio.grpc.CloseUfsFilePResponse> METHOD_CLOSE_UFS_FILE = getCloseUfsFileMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CloseUfsFilePRequest,
      alluxio.grpc.CloseUfsFilePResponse> getCloseUfsFileMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.CloseUfsFilePRequest,
      alluxio.grpc.CloseUfsFilePResponse> getCloseUfsFileMethod() {
    return getCloseUfsFileMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.CloseUfsFilePRequest,
      alluxio.grpc.CloseUfsFilePResponse> getCloseUfsFileMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.CloseUfsFilePRequest, alluxio.grpc.CloseUfsFilePResponse> getCloseUfsFileMethod;
    if ((getCloseUfsFileMethod = FileSystemWorkerClientServiceGrpc.getCloseUfsFileMethod) == null) {
      synchronized (FileSystemWorkerClientServiceGrpc.class) {
        if ((getCloseUfsFileMethod = FileSystemWorkerClientServiceGrpc.getCloseUfsFileMethod) == null) {
          FileSystemWorkerClientServiceGrpc.getCloseUfsFileMethod = getCloseUfsFileMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CloseUfsFilePRequest, alluxio.grpc.CloseUfsFilePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.FileSystemWorkerClientService", "CloseUfsFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CloseUfsFilePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CloseUfsFilePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemWorkerClientServiceMethodDescriptorSupplier("CloseUfsFile"))
                  .build();
          }
        }
     }
     return getCloseUfsFileMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getCompleteUfsFileMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.CompleteUfsFilePRequest,
      alluxio.grpc.CompleteUfsFilePReponse> METHOD_COMPLETE_UFS_FILE = getCompleteUfsFileMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CompleteUfsFilePRequest,
      alluxio.grpc.CompleteUfsFilePReponse> getCompleteUfsFileMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.CompleteUfsFilePRequest,
      alluxio.grpc.CompleteUfsFilePReponse> getCompleteUfsFileMethod() {
    return getCompleteUfsFileMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.CompleteUfsFilePRequest,
      alluxio.grpc.CompleteUfsFilePReponse> getCompleteUfsFileMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.CompleteUfsFilePRequest, alluxio.grpc.CompleteUfsFilePReponse> getCompleteUfsFileMethod;
    if ((getCompleteUfsFileMethod = FileSystemWorkerClientServiceGrpc.getCompleteUfsFileMethod) == null) {
      synchronized (FileSystemWorkerClientServiceGrpc.class) {
        if ((getCompleteUfsFileMethod = FileSystemWorkerClientServiceGrpc.getCompleteUfsFileMethod) == null) {
          FileSystemWorkerClientServiceGrpc.getCompleteUfsFileMethod = getCompleteUfsFileMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CompleteUfsFilePRequest, alluxio.grpc.CompleteUfsFilePReponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.FileSystemWorkerClientService", "CompleteUfsFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CompleteUfsFilePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CompleteUfsFilePReponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemWorkerClientServiceMethodDescriptorSupplier("CompleteUfsFile"))
                  .build();
          }
        }
     }
     return getCompleteUfsFileMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getCreateUfsFileMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.CreateUfsFilePRequest,
      alluxio.grpc.CreateUfsFilePResponse> METHOD_CREATE_UFS_FILE = getCreateUfsFileMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CreateUfsFilePRequest,
      alluxio.grpc.CreateUfsFilePResponse> getCreateUfsFileMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.CreateUfsFilePRequest,
      alluxio.grpc.CreateUfsFilePResponse> getCreateUfsFileMethod() {
    return getCreateUfsFileMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.CreateUfsFilePRequest,
      alluxio.grpc.CreateUfsFilePResponse> getCreateUfsFileMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.CreateUfsFilePRequest, alluxio.grpc.CreateUfsFilePResponse> getCreateUfsFileMethod;
    if ((getCreateUfsFileMethod = FileSystemWorkerClientServiceGrpc.getCreateUfsFileMethod) == null) {
      synchronized (FileSystemWorkerClientServiceGrpc.class) {
        if ((getCreateUfsFileMethod = FileSystemWorkerClientServiceGrpc.getCreateUfsFileMethod) == null) {
          FileSystemWorkerClientServiceGrpc.getCreateUfsFileMethod = getCreateUfsFileMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CreateUfsFilePRequest, alluxio.grpc.CreateUfsFilePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.FileSystemWorkerClientService", "CreateUfsFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateUfsFilePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateUfsFilePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemWorkerClientServiceMethodDescriptorSupplier("CreateUfsFile"))
                  .build();
          }
        }
     }
     return getCreateUfsFileMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getOpenUfsFileMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.OpenUfsFilePRequest,
      alluxio.grpc.OpenUfsFilePResponse> METHOD_OPEN_UFS_FILE = getOpenUfsFileMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.OpenUfsFilePRequest,
      alluxio.grpc.OpenUfsFilePResponse> getOpenUfsFileMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.OpenUfsFilePRequest,
      alluxio.grpc.OpenUfsFilePResponse> getOpenUfsFileMethod() {
    return getOpenUfsFileMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.OpenUfsFilePRequest,
      alluxio.grpc.OpenUfsFilePResponse> getOpenUfsFileMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.OpenUfsFilePRequest, alluxio.grpc.OpenUfsFilePResponse> getOpenUfsFileMethod;
    if ((getOpenUfsFileMethod = FileSystemWorkerClientServiceGrpc.getOpenUfsFileMethod) == null) {
      synchronized (FileSystemWorkerClientServiceGrpc.class) {
        if ((getOpenUfsFileMethod = FileSystemWorkerClientServiceGrpc.getOpenUfsFileMethod) == null) {
          FileSystemWorkerClientServiceGrpc.getOpenUfsFileMethod = getOpenUfsFileMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.OpenUfsFilePRequest, alluxio.grpc.OpenUfsFilePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.FileSystemWorkerClientService", "OpenUfsFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.OpenUfsFilePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.OpenUfsFilePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemWorkerClientServiceMethodDescriptorSupplier("OpenUfsFile"))
                  .build();
          }
        }
     }
     return getOpenUfsFileMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getSessionFileSystemHeartbeatMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.SessionFileSystemHeartbeatPRequest,
      alluxio.grpc.SessionFileSystemHeartbeatPResponse> METHOD_SESSION_FILE_SYSTEM_HEARTBEAT = getSessionFileSystemHeartbeatMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.SessionFileSystemHeartbeatPRequest,
      alluxio.grpc.SessionFileSystemHeartbeatPResponse> getSessionFileSystemHeartbeatMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.SessionFileSystemHeartbeatPRequest,
      alluxio.grpc.SessionFileSystemHeartbeatPResponse> getSessionFileSystemHeartbeatMethod() {
    return getSessionFileSystemHeartbeatMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.SessionFileSystemHeartbeatPRequest,
      alluxio.grpc.SessionFileSystemHeartbeatPResponse> getSessionFileSystemHeartbeatMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.SessionFileSystemHeartbeatPRequest, alluxio.grpc.SessionFileSystemHeartbeatPResponse> getSessionFileSystemHeartbeatMethod;
    if ((getSessionFileSystemHeartbeatMethod = FileSystemWorkerClientServiceGrpc.getSessionFileSystemHeartbeatMethod) == null) {
      synchronized (FileSystemWorkerClientServiceGrpc.class) {
        if ((getSessionFileSystemHeartbeatMethod = FileSystemWorkerClientServiceGrpc.getSessionFileSystemHeartbeatMethod) == null) {
          FileSystemWorkerClientServiceGrpc.getSessionFileSystemHeartbeatMethod = getSessionFileSystemHeartbeatMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.SessionFileSystemHeartbeatPRequest, alluxio.grpc.SessionFileSystemHeartbeatPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.FileSystemWorkerClientService", "SessionFileSystemHeartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SessionFileSystemHeartbeatPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SessionFileSystemHeartbeatPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemWorkerClientServiceMethodDescriptorSupplier("SessionFileSystemHeartbeat"))
                  .build();
          }
        }
     }
     return getSessionFileSystemHeartbeatMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FileSystemWorkerClientServiceStub newStub(io.grpc.Channel channel) {
    return new FileSystemWorkerClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FileSystemWorkerClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new FileSystemWorkerClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static FileSystemWorkerClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new FileSystemWorkerClientServiceFutureStub(channel);
  }

  /**
   * <pre>
   **
   * This interface contains file system worker service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class FileSystemWorkerClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Cancels a file which has not been completed in the under file system.
     * </pre>
     */
    public void cancelUfsFile(alluxio.grpc.CancelUfsFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CancelUfsFilePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCancelUfsFileMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Closes a file in the under file system which was previously opened for reading.
     * </pre>
     */
    public void closeUfsFile(alluxio.grpc.CloseUfsFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CloseUfsFilePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCloseUfsFileMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Completes a file in the under file system.
     * </pre>
     */
    public void completeUfsFile(alluxio.grpc.CompleteUfsFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CompleteUfsFilePReponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCompleteUfsFileMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a file in the under file system.
     * </pre>
     */
    public void createUfsFile(alluxio.grpc.CreateUfsFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateUfsFilePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateUfsFileMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Opens an existing file in the under file system for reading.
     * </pre>
     */
    public void openUfsFile(alluxio.grpc.OpenUfsFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.OpenUfsFilePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getOpenUfsFileMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Local session send heartbeat to local worker to keep its state.
     * </pre>
     */
    public void sessionFileSystemHeartbeat(alluxio.grpc.SessionFileSystemHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SessionFileSystemHeartbeatPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSessionFileSystemHeartbeatMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCancelUfsFileMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CancelUfsFilePRequest,
                alluxio.grpc.CancelUfsFilePResponse>(
                  this, METHODID_CANCEL_UFS_FILE)))
          .addMethod(
            getCloseUfsFileMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CloseUfsFilePRequest,
                alluxio.grpc.CloseUfsFilePResponse>(
                  this, METHODID_CLOSE_UFS_FILE)))
          .addMethod(
            getCompleteUfsFileMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CompleteUfsFilePRequest,
                alluxio.grpc.CompleteUfsFilePReponse>(
                  this, METHODID_COMPLETE_UFS_FILE)))
          .addMethod(
            getCreateUfsFileMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CreateUfsFilePRequest,
                alluxio.grpc.CreateUfsFilePResponse>(
                  this, METHODID_CREATE_UFS_FILE)))
          .addMethod(
            getOpenUfsFileMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.OpenUfsFilePRequest,
                alluxio.grpc.OpenUfsFilePResponse>(
                  this, METHODID_OPEN_UFS_FILE)))
          .addMethod(
            getSessionFileSystemHeartbeatMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.SessionFileSystemHeartbeatPRequest,
                alluxio.grpc.SessionFileSystemHeartbeatPResponse>(
                  this, METHODID_SESSION_FILE_SYSTEM_HEARTBEAT)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system worker service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class FileSystemWorkerClientServiceStub extends io.grpc.stub.AbstractStub<FileSystemWorkerClientServiceStub> {
    private FileSystemWorkerClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemWorkerClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemWorkerClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemWorkerClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Cancels a file which has not been completed in the under file system.
     * </pre>
     */
    public void cancelUfsFile(alluxio.grpc.CancelUfsFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CancelUfsFilePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCancelUfsFileMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Closes a file in the under file system which was previously opened for reading.
     * </pre>
     */
    public void closeUfsFile(alluxio.grpc.CloseUfsFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CloseUfsFilePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCloseUfsFileMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Completes a file in the under file system.
     * </pre>
     */
    public void completeUfsFile(alluxio.grpc.CompleteUfsFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CompleteUfsFilePReponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCompleteUfsFileMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a file in the under file system.
     * </pre>
     */
    public void createUfsFile(alluxio.grpc.CreateUfsFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateUfsFilePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateUfsFileMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Opens an existing file in the under file system for reading.
     * </pre>
     */
    public void openUfsFile(alluxio.grpc.OpenUfsFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.OpenUfsFilePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getOpenUfsFileMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Local session send heartbeat to local worker to keep its state.
     * </pre>
     */
    public void sessionFileSystemHeartbeat(alluxio.grpc.SessionFileSystemHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SessionFileSystemHeartbeatPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSessionFileSystemHeartbeatMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system worker service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class FileSystemWorkerClientServiceBlockingStub extends io.grpc.stub.AbstractStub<FileSystemWorkerClientServiceBlockingStub> {
    private FileSystemWorkerClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemWorkerClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemWorkerClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemWorkerClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Cancels a file which has not been completed in the under file system.
     * </pre>
     */
    public alluxio.grpc.CancelUfsFilePResponse cancelUfsFile(alluxio.grpc.CancelUfsFilePRequest request) {
      return blockingUnaryCall(
          getChannel(), getCancelUfsFileMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Closes a file in the under file system which was previously opened for reading.
     * </pre>
     */
    public alluxio.grpc.CloseUfsFilePResponse closeUfsFile(alluxio.grpc.CloseUfsFilePRequest request) {
      return blockingUnaryCall(
          getChannel(), getCloseUfsFileMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Completes a file in the under file system.
     * </pre>
     */
    public alluxio.grpc.CompleteUfsFilePReponse completeUfsFile(alluxio.grpc.CompleteUfsFilePRequest request) {
      return blockingUnaryCall(
          getChannel(), getCompleteUfsFileMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Creates a file in the under file system.
     * </pre>
     */
    public alluxio.grpc.CreateUfsFilePResponse createUfsFile(alluxio.grpc.CreateUfsFilePRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateUfsFileMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Opens an existing file in the under file system for reading.
     * </pre>
     */
    public alluxio.grpc.OpenUfsFilePResponse openUfsFile(alluxio.grpc.OpenUfsFilePRequest request) {
      return blockingUnaryCall(
          getChannel(), getOpenUfsFileMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Local session send heartbeat to local worker to keep its state.
     * </pre>
     */
    public alluxio.grpc.SessionFileSystemHeartbeatPResponse sessionFileSystemHeartbeat(alluxio.grpc.SessionFileSystemHeartbeatPRequest request) {
      return blockingUnaryCall(
          getChannel(), getSessionFileSystemHeartbeatMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system worker service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class FileSystemWorkerClientServiceFutureStub extends io.grpc.stub.AbstractStub<FileSystemWorkerClientServiceFutureStub> {
    private FileSystemWorkerClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemWorkerClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemWorkerClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemWorkerClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Cancels a file which has not been completed in the under file system.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CancelUfsFilePResponse> cancelUfsFile(
        alluxio.grpc.CancelUfsFilePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCancelUfsFileMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Closes a file in the under file system which was previously opened for reading.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CloseUfsFilePResponse> closeUfsFile(
        alluxio.grpc.CloseUfsFilePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCloseUfsFileMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Completes a file in the under file system.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CompleteUfsFilePReponse> completeUfsFile(
        alluxio.grpc.CompleteUfsFilePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCompleteUfsFileMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Creates a file in the under file system.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CreateUfsFilePResponse> createUfsFile(
        alluxio.grpc.CreateUfsFilePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateUfsFileMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Opens an existing file in the under file system for reading.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.OpenUfsFilePResponse> openUfsFile(
        alluxio.grpc.OpenUfsFilePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getOpenUfsFileMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Local session send heartbeat to local worker to keep its state.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.SessionFileSystemHeartbeatPResponse> sessionFileSystemHeartbeat(
        alluxio.grpc.SessionFileSystemHeartbeatPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSessionFileSystemHeartbeatMethodHelper(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CANCEL_UFS_FILE = 0;
  private static final int METHODID_CLOSE_UFS_FILE = 1;
  private static final int METHODID_COMPLETE_UFS_FILE = 2;
  private static final int METHODID_CREATE_UFS_FILE = 3;
  private static final int METHODID_OPEN_UFS_FILE = 4;
  private static final int METHODID_SESSION_FILE_SYSTEM_HEARTBEAT = 5;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final FileSystemWorkerClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(FileSystemWorkerClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CANCEL_UFS_FILE:
          serviceImpl.cancelUfsFile((alluxio.grpc.CancelUfsFilePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CancelUfsFilePResponse>) responseObserver);
          break;
        case METHODID_CLOSE_UFS_FILE:
          serviceImpl.closeUfsFile((alluxio.grpc.CloseUfsFilePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CloseUfsFilePResponse>) responseObserver);
          break;
        case METHODID_COMPLETE_UFS_FILE:
          serviceImpl.completeUfsFile((alluxio.grpc.CompleteUfsFilePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CompleteUfsFilePReponse>) responseObserver);
          break;
        case METHODID_CREATE_UFS_FILE:
          serviceImpl.createUfsFile((alluxio.grpc.CreateUfsFilePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CreateUfsFilePResponse>) responseObserver);
          break;
        case METHODID_OPEN_UFS_FILE:
          serviceImpl.openUfsFile((alluxio.grpc.OpenUfsFilePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.OpenUfsFilePResponse>) responseObserver);
          break;
        case METHODID_SESSION_FILE_SYSTEM_HEARTBEAT:
          serviceImpl.sessionFileSystemHeartbeat((alluxio.grpc.SessionFileSystemHeartbeatPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.SessionFileSystemHeartbeatPResponse>) responseObserver);
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

  private static abstract class FileSystemWorkerClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    FileSystemWorkerClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.FileSystemWorkerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("FileSystemWorkerClientService");
    }
  }

  private static final class FileSystemWorkerClientServiceFileDescriptorSupplier
      extends FileSystemWorkerClientServiceBaseDescriptorSupplier {
    FileSystemWorkerClientServiceFileDescriptorSupplier() {}
  }

  private static final class FileSystemWorkerClientServiceMethodDescriptorSupplier
      extends FileSystemWorkerClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    FileSystemWorkerClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (FileSystemWorkerClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new FileSystemWorkerClientServiceFileDescriptorSupplier())
              .addMethod(getCancelUfsFileMethodHelper())
              .addMethod(getCloseUfsFileMethodHelper())
              .addMethod(getCompleteUfsFileMethodHelper())
              .addMethod(getCreateUfsFileMethodHelper())
              .addMethod(getOpenUfsFileMethodHelper())
              .addMethod(getSessionFileSystemHeartbeatMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
