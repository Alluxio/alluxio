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
 * This interface contains file system master service endpoints for Alluxio workers.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.10.1)",
    comments = "Source: file_system_master.proto")
public final class FileSystemMasterWorkerServiceGrpc {

  private FileSystemMasterWorkerServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.FileSystemMasterWorkerService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getFileSystemHeartbeatMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.FileSystemHeartbeatPRequest,
      alluxio.grpc.FileSystemHeartbeatPResponse> METHOD_FILE_SYSTEM_HEARTBEAT = getFileSystemHeartbeatMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.FileSystemHeartbeatPRequest,
      alluxio.grpc.FileSystemHeartbeatPResponse> getFileSystemHeartbeatMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.FileSystemHeartbeatPRequest,
      alluxio.grpc.FileSystemHeartbeatPResponse> getFileSystemHeartbeatMethod() {
    return getFileSystemHeartbeatMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.FileSystemHeartbeatPRequest,
      alluxio.grpc.FileSystemHeartbeatPResponse> getFileSystemHeartbeatMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.FileSystemHeartbeatPRequest, alluxio.grpc.FileSystemHeartbeatPResponse> getFileSystemHeartbeatMethod;
    if ((getFileSystemHeartbeatMethod = FileSystemMasterWorkerServiceGrpc.getFileSystemHeartbeatMethod) == null) {
      synchronized (FileSystemMasterWorkerServiceGrpc.class) {
        if ((getFileSystemHeartbeatMethod = FileSystemMasterWorkerServiceGrpc.getFileSystemHeartbeatMethod) == null) {
          FileSystemMasterWorkerServiceGrpc.getFileSystemHeartbeatMethod = getFileSystemHeartbeatMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.FileSystemHeartbeatPRequest, alluxio.grpc.FileSystemHeartbeatPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.FileSystemMasterWorkerService", "FileSystemHeartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.FileSystemHeartbeatPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.FileSystemHeartbeatPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemMasterWorkerServiceMethodDescriptorSupplier("FileSystemHeartbeat"))
                  .build();
          }
        }
     }
     return getFileSystemHeartbeatMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetFileInfoMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetFileInfoPRequest,
      alluxio.grpc.GetFileInfoPResponse> METHOD_GET_FILE_INFO = getGetFileInfoMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetFileInfoPRequest,
      alluxio.grpc.GetFileInfoPResponse> getGetFileInfoMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetFileInfoPRequest,
      alluxio.grpc.GetFileInfoPResponse> getGetFileInfoMethod() {
    return getGetFileInfoMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetFileInfoPRequest,
      alluxio.grpc.GetFileInfoPResponse> getGetFileInfoMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetFileInfoPRequest, alluxio.grpc.GetFileInfoPResponse> getGetFileInfoMethod;
    if ((getGetFileInfoMethod = FileSystemMasterWorkerServiceGrpc.getGetFileInfoMethod) == null) {
      synchronized (FileSystemMasterWorkerServiceGrpc.class) {
        if ((getGetFileInfoMethod = FileSystemMasterWorkerServiceGrpc.getGetFileInfoMethod) == null) {
          FileSystemMasterWorkerServiceGrpc.getGetFileInfoMethod = getGetFileInfoMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetFileInfoPRequest, alluxio.grpc.GetFileInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.FileSystemMasterWorkerService", "GetFileInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetFileInfoPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetFileInfoPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemMasterWorkerServiceMethodDescriptorSupplier("GetFileInfo"))
                  .build();
          }
        }
     }
     return getGetFileInfoMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetPinnedFileIdsMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetPinnedFileIdsPRequest,
      alluxio.grpc.GetPinnedFileIdsPResponse> METHOD_GET_PINNED_FILE_IDS = getGetPinnedFileIdsMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetPinnedFileIdsPRequest,
      alluxio.grpc.GetPinnedFileIdsPResponse> getGetPinnedFileIdsMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetPinnedFileIdsPRequest,
      alluxio.grpc.GetPinnedFileIdsPResponse> getGetPinnedFileIdsMethod() {
    return getGetPinnedFileIdsMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetPinnedFileIdsPRequest,
      alluxio.grpc.GetPinnedFileIdsPResponse> getGetPinnedFileIdsMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetPinnedFileIdsPRequest, alluxio.grpc.GetPinnedFileIdsPResponse> getGetPinnedFileIdsMethod;
    if ((getGetPinnedFileIdsMethod = FileSystemMasterWorkerServiceGrpc.getGetPinnedFileIdsMethod) == null) {
      synchronized (FileSystemMasterWorkerServiceGrpc.class) {
        if ((getGetPinnedFileIdsMethod = FileSystemMasterWorkerServiceGrpc.getGetPinnedFileIdsMethod) == null) {
          FileSystemMasterWorkerServiceGrpc.getGetPinnedFileIdsMethod = getGetPinnedFileIdsMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetPinnedFileIdsPRequest, alluxio.grpc.GetPinnedFileIdsPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.FileSystemMasterWorkerService", "GetPinnedFileIds"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetPinnedFileIdsPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetPinnedFileIdsPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemMasterWorkerServiceMethodDescriptorSupplier("GetPinnedFileIds"))
                  .build();
          }
        }
     }
     return getGetPinnedFileIdsMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetUfsInfoMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetUfsInfoPRequest,
      alluxio.grpc.GetUfsInfoPResponse> METHOD_GET_UFS_INFO = getGetUfsInfoMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetUfsInfoPRequest,
      alluxio.grpc.GetUfsInfoPResponse> getGetUfsInfoMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetUfsInfoPRequest,
      alluxio.grpc.GetUfsInfoPResponse> getGetUfsInfoMethod() {
    return getGetUfsInfoMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetUfsInfoPRequest,
      alluxio.grpc.GetUfsInfoPResponse> getGetUfsInfoMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetUfsInfoPRequest, alluxio.grpc.GetUfsInfoPResponse> getGetUfsInfoMethod;
    if ((getGetUfsInfoMethod = FileSystemMasterWorkerServiceGrpc.getGetUfsInfoMethod) == null) {
      synchronized (FileSystemMasterWorkerServiceGrpc.class) {
        if ((getGetUfsInfoMethod = FileSystemMasterWorkerServiceGrpc.getGetUfsInfoMethod) == null) {
          FileSystemMasterWorkerServiceGrpc.getGetUfsInfoMethod = getGetUfsInfoMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetUfsInfoPRequest, alluxio.grpc.GetUfsInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.FileSystemMasterWorkerService", "GetUfsInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetUfsInfoPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetUfsInfoPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemMasterWorkerServiceMethodDescriptorSupplier("GetUfsInfo"))
                  .build();
          }
        }
     }
     return getGetUfsInfoMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FileSystemMasterWorkerServiceStub newStub(io.grpc.Channel channel) {
    return new FileSystemMasterWorkerServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FileSystemMasterWorkerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new FileSystemMasterWorkerServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static FileSystemMasterWorkerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new FileSystemMasterWorkerServiceFutureStub(channel);
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio workers.
   * </pre>
   */
  public static abstract class FileSystemMasterWorkerServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Periodic file system worker heartbeat. Returns the command for persisting
     * the blocks of a file.
     * </pre>
     */
    public void fileSystemHeartbeat(alluxio.grpc.FileSystemHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.FileSystemHeartbeatPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getFileSystemHeartbeatMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     * Returns the file information for a file or directory identified by the given file id.
     * </pre>
     */
    public void getFileInfo(alluxio.grpc.GetFileInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetFileInfoPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetFileInfoMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the set of pinned file ids.
     * </pre>
     */
    public void getPinnedFileIds(alluxio.grpc.GetPinnedFileIdsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetPinnedFileIdsPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetPinnedFileIdsMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the UFS information for the given mount point identified by its id.
     * </pre>
     */
    public void getUfsInfo(alluxio.grpc.GetUfsInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetUfsInfoPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetUfsInfoMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getFileSystemHeartbeatMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.FileSystemHeartbeatPRequest,
                alluxio.grpc.FileSystemHeartbeatPResponse>(
                  this, METHODID_FILE_SYSTEM_HEARTBEAT)))
          .addMethod(
            getGetFileInfoMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetFileInfoPRequest,
                alluxio.grpc.GetFileInfoPResponse>(
                  this, METHODID_GET_FILE_INFO)))
          .addMethod(
            getGetPinnedFileIdsMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetPinnedFileIdsPRequest,
                alluxio.grpc.GetPinnedFileIdsPResponse>(
                  this, METHODID_GET_PINNED_FILE_IDS)))
          .addMethod(
            getGetUfsInfoMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetUfsInfoPRequest,
                alluxio.grpc.GetUfsInfoPResponse>(
                  this, METHODID_GET_UFS_INFO)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio workers.
   * </pre>
   */
  public static final class FileSystemMasterWorkerServiceStub extends io.grpc.stub.AbstractStub<FileSystemMasterWorkerServiceStub> {
    private FileSystemMasterWorkerServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterWorkerServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterWorkerServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterWorkerServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Periodic file system worker heartbeat. Returns the command for persisting
     * the blocks of a file.
     * </pre>
     */
    public void fileSystemHeartbeat(alluxio.grpc.FileSystemHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.FileSystemHeartbeatPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getFileSystemHeartbeatMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Returns the file information for a file or directory identified by the given file id.
     * </pre>
     */
    public void getFileInfo(alluxio.grpc.GetFileInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetFileInfoPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetFileInfoMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the set of pinned file ids.
     * </pre>
     */
    public void getPinnedFileIds(alluxio.grpc.GetPinnedFileIdsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetPinnedFileIdsPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetPinnedFileIdsMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the UFS information for the given mount point identified by its id.
     * </pre>
     */
    public void getUfsInfo(alluxio.grpc.GetUfsInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetUfsInfoPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetUfsInfoMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio workers.
   * </pre>
   */
  public static final class FileSystemMasterWorkerServiceBlockingStub extends io.grpc.stub.AbstractStub<FileSystemMasterWorkerServiceBlockingStub> {
    private FileSystemMasterWorkerServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterWorkerServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterWorkerServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterWorkerServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Periodic file system worker heartbeat. Returns the command for persisting
     * the blocks of a file.
     * </pre>
     */
    public alluxio.grpc.FileSystemHeartbeatPResponse fileSystemHeartbeat(alluxio.grpc.FileSystemHeartbeatPRequest request) {
      return blockingUnaryCall(
          getChannel(), getFileSystemHeartbeatMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Returns the file information for a file or directory identified by the given file id.
     * </pre>
     */
    public alluxio.grpc.GetFileInfoPResponse getFileInfo(alluxio.grpc.GetFileInfoPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetFileInfoMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the set of pinned file ids.
     * </pre>
     */
    public alluxio.grpc.GetPinnedFileIdsPResponse getPinnedFileIds(alluxio.grpc.GetPinnedFileIdsPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetPinnedFileIdsMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the UFS information for the given mount point identified by its id.
     * </pre>
     */
    public alluxio.grpc.GetUfsInfoPResponse getUfsInfo(alluxio.grpc.GetUfsInfoPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetUfsInfoMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio workers.
   * </pre>
   */
  public static final class FileSystemMasterWorkerServiceFutureStub extends io.grpc.stub.AbstractStub<FileSystemMasterWorkerServiceFutureStub> {
    private FileSystemMasterWorkerServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterWorkerServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterWorkerServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterWorkerServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Periodic file system worker heartbeat. Returns the command for persisting
     * the blocks of a file.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.FileSystemHeartbeatPResponse> fileSystemHeartbeat(
        alluxio.grpc.FileSystemHeartbeatPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getFileSystemHeartbeatMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Returns the file information for a file or directory identified by the given file id.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetFileInfoPResponse> getFileInfo(
        alluxio.grpc.GetFileInfoPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetFileInfoMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the set of pinned file ids.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetPinnedFileIdsPResponse> getPinnedFileIds(
        alluxio.grpc.GetPinnedFileIdsPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetPinnedFileIdsMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the UFS information for the given mount point identified by its id.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetUfsInfoPResponse> getUfsInfo(
        alluxio.grpc.GetUfsInfoPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetUfsInfoMethodHelper(), getCallOptions()), request);
    }
  }

  private static final int METHODID_FILE_SYSTEM_HEARTBEAT = 0;
  private static final int METHODID_GET_FILE_INFO = 1;
  private static final int METHODID_GET_PINNED_FILE_IDS = 2;
  private static final int METHODID_GET_UFS_INFO = 3;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final FileSystemMasterWorkerServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(FileSystemMasterWorkerServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_FILE_SYSTEM_HEARTBEAT:
          serviceImpl.fileSystemHeartbeat((alluxio.grpc.FileSystemHeartbeatPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.FileSystemHeartbeatPResponse>) responseObserver);
          break;
        case METHODID_GET_FILE_INFO:
          serviceImpl.getFileInfo((alluxio.grpc.GetFileInfoPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetFileInfoPResponse>) responseObserver);
          break;
        case METHODID_GET_PINNED_FILE_IDS:
          serviceImpl.getPinnedFileIds((alluxio.grpc.GetPinnedFileIdsPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetPinnedFileIdsPResponse>) responseObserver);
          break;
        case METHODID_GET_UFS_INFO:
          serviceImpl.getUfsInfo((alluxio.grpc.GetUfsInfoPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetUfsInfoPResponse>) responseObserver);
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

  private static abstract class FileSystemMasterWorkerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    FileSystemMasterWorkerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("FileSystemMasterWorkerService");
    }
  }

  private static final class FileSystemMasterWorkerServiceFileDescriptorSupplier
      extends FileSystemMasterWorkerServiceBaseDescriptorSupplier {
    FileSystemMasterWorkerServiceFileDescriptorSupplier() {}
  }

  private static final class FileSystemMasterWorkerServiceMethodDescriptorSupplier
      extends FileSystemMasterWorkerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    FileSystemMasterWorkerServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (FileSystemMasterWorkerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new FileSystemMasterWorkerServiceFileDescriptorSupplier())
              .addMethod(getFileSystemHeartbeatMethodHelper())
              .addMethod(getGetFileInfoMethodHelper())
              .addMethod(getGetPinnedFileIdsMethodHelper())
              .addMethod(getGetUfsInfoMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
