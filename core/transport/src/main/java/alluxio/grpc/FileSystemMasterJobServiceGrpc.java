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
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: grpc/file_system_master.proto")
public final class FileSystemMasterJobServiceGrpc {

  private FileSystemMasterJobServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.file.FileSystemMasterJobService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetFileInfoPRequest,
      alluxio.grpc.GetFileInfoPResponse> getGetFileInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetFileInfo",
      requestType = alluxio.grpc.GetFileInfoPRequest.class,
      responseType = alluxio.grpc.GetFileInfoPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetFileInfoPRequest,
      alluxio.grpc.GetFileInfoPResponse> getGetFileInfoMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetFileInfoPRequest, alluxio.grpc.GetFileInfoPResponse> getGetFileInfoMethod;
    if ((getGetFileInfoMethod = FileSystemMasterJobServiceGrpc.getGetFileInfoMethod) == null) {
      synchronized (FileSystemMasterJobServiceGrpc.class) {
        if ((getGetFileInfoMethod = FileSystemMasterJobServiceGrpc.getGetFileInfoMethod) == null) {
          FileSystemMasterJobServiceGrpc.getGetFileInfoMethod = getGetFileInfoMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetFileInfoPRequest, alluxio.grpc.GetFileInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterJobService", "GetFileInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetFileInfoPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetFileInfoPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemMasterJobServiceMethodDescriptorSupplier("GetFileInfo"))
                  .build();
          }
        }
     }
     return getGetFileInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetUfsInfoPRequest,
      alluxio.grpc.GetUfsInfoPResponse> getGetUfsInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetUfsInfo",
      requestType = alluxio.grpc.GetUfsInfoPRequest.class,
      responseType = alluxio.grpc.GetUfsInfoPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetUfsInfoPRequest,
      alluxio.grpc.GetUfsInfoPResponse> getGetUfsInfoMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetUfsInfoPRequest, alluxio.grpc.GetUfsInfoPResponse> getGetUfsInfoMethod;
    if ((getGetUfsInfoMethod = FileSystemMasterJobServiceGrpc.getGetUfsInfoMethod) == null) {
      synchronized (FileSystemMasterJobServiceGrpc.class) {
        if ((getGetUfsInfoMethod = FileSystemMasterJobServiceGrpc.getGetUfsInfoMethod) == null) {
          FileSystemMasterJobServiceGrpc.getGetUfsInfoMethod = getGetUfsInfoMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetUfsInfoPRequest, alluxio.grpc.GetUfsInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterJobService", "GetUfsInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetUfsInfoPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetUfsInfoPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemMasterJobServiceMethodDescriptorSupplier("GetUfsInfo"))
                  .build();
          }
        }
     }
     return getGetUfsInfoMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FileSystemMasterJobServiceStub newStub(io.grpc.Channel channel) {
    return new FileSystemMasterJobServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FileSystemMasterJobServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new FileSystemMasterJobServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static FileSystemMasterJobServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new FileSystemMasterJobServiceFutureStub(channel);
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio workers.
   * </pre>
   */
  public static abstract class FileSystemMasterJobServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Returns the file information for a file or directory identified by the given file id.
     * </pre>
     */
    public void getFileInfo(alluxio.grpc.GetFileInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetFileInfoPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetFileInfoMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the UFS information for the given mount point identified by its id.
     * </pre>
     */
    public void getUfsInfo(alluxio.grpc.GetUfsInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetUfsInfoPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetUfsInfoMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetFileInfoMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetFileInfoPRequest,
                alluxio.grpc.GetFileInfoPResponse>(
                  this, METHODID_GET_FILE_INFO)))
          .addMethod(
            getGetUfsInfoMethod(),
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
  public static final class FileSystemMasterJobServiceStub extends io.grpc.stub.AbstractStub<FileSystemMasterJobServiceStub> {
    private FileSystemMasterJobServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterJobServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterJobServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterJobServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Returns the file information for a file or directory identified by the given file id.
     * </pre>
     */
    public void getFileInfo(alluxio.grpc.GetFileInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetFileInfoPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetFileInfoMethod(), getCallOptions()), request, responseObserver);
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
          getChannel().newCall(getGetUfsInfoMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio workers.
   * </pre>
   */
  public static final class FileSystemMasterJobServiceBlockingStub extends io.grpc.stub.AbstractStub<FileSystemMasterJobServiceBlockingStub> {
    private FileSystemMasterJobServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterJobServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterJobServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterJobServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Returns the file information for a file or directory identified by the given file id.
     * </pre>
     */
    public alluxio.grpc.GetFileInfoPResponse getFileInfo(alluxio.grpc.GetFileInfoPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetFileInfoMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the UFS information for the given mount point identified by its id.
     * </pre>
     */
    public alluxio.grpc.GetUfsInfoPResponse getUfsInfo(alluxio.grpc.GetUfsInfoPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetUfsInfoMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio workers.
   * </pre>
   */
  public static final class FileSystemMasterJobServiceFutureStub extends io.grpc.stub.AbstractStub<FileSystemMasterJobServiceFutureStub> {
    private FileSystemMasterJobServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterJobServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterJobServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterJobServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Returns the file information for a file or directory identified by the given file id.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetFileInfoPResponse> getFileInfo(
        alluxio.grpc.GetFileInfoPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetFileInfoMethod(), getCallOptions()), request);
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
          getChannel().newCall(getGetUfsInfoMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_FILE_INFO = 0;
  private static final int METHODID_GET_UFS_INFO = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final FileSystemMasterJobServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(FileSystemMasterJobServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_FILE_INFO:
          serviceImpl.getFileInfo((alluxio.grpc.GetFileInfoPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetFileInfoPResponse>) responseObserver);
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

  private static abstract class FileSystemMasterJobServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    FileSystemMasterJobServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("FileSystemMasterJobService");
    }
  }

  private static final class FileSystemMasterJobServiceFileDescriptorSupplier
      extends FileSystemMasterJobServiceBaseDescriptorSupplier {
    FileSystemMasterJobServiceFileDescriptorSupplier() {}
  }

  private static final class FileSystemMasterJobServiceMethodDescriptorSupplier
      extends FileSystemMasterJobServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    FileSystemMasterJobServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (FileSystemMasterJobServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new FileSystemMasterJobServiceFileDescriptorSupplier())
              .addMethod(getGetFileInfoMethod())
              .addMethod(getGetUfsInfoMethod())
              .build();
        }
      }
    }
    return result;
  }
}
