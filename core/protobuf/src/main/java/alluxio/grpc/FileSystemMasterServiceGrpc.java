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
    comments = "Source: file_system_master.proto")
public final class FileSystemMasterServiceGrpc {

  private FileSystemMasterServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.FileSystemMasterService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetStatusMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetStatusPRequest,
      alluxio.grpc.GetStatusPResponse> METHOD_GET_STATUS = getGetStatusMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetStatusPRequest,
      alluxio.grpc.GetStatusPResponse> getGetStatusMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetStatusPRequest,
      alluxio.grpc.GetStatusPResponse> getGetStatusMethod() {
    return getGetStatusMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetStatusPRequest,
      alluxio.grpc.GetStatusPResponse> getGetStatusMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetStatusPRequest, alluxio.grpc.GetStatusPResponse> getGetStatusMethod;
    if ((getGetStatusMethod = FileSystemMasterServiceGrpc.getGetStatusMethod) == null) {
      synchronized (FileSystemMasterServiceGrpc.class) {
        if ((getGetStatusMethod = FileSystemMasterServiceGrpc.getGetStatusMethod) == null) {
          FileSystemMasterServiceGrpc.getGetStatusMethod = getGetStatusMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetStatusPRequest, alluxio.grpc.GetStatusPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.FileSystemMasterService", "GetStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetStatusPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetStatusPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new FileSystemMasterServiceMethodDescriptorSupplier("GetStatus"))
                  .build();
          }
        }
     }
     return getGetStatusMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FileSystemMasterServiceStub newStub(io.grpc.Channel channel) {
    return new FileSystemMasterServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FileSystemMasterServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new FileSystemMasterServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static FileSystemMasterServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new FileSystemMasterServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class FileSystemMasterServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getStatus(alluxio.grpc.GetStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetStatusPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetStatusMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetStatusMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetStatusPRequest,
                alluxio.grpc.GetStatusPResponse>(
                  this, METHODID_GET_STATUS)))
          .build();
    }
  }

  /**
   */
  public static final class FileSystemMasterServiceStub extends io.grpc.stub.AbstractStub<FileSystemMasterServiceStub> {
    private FileSystemMasterServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterServiceStub(channel, callOptions);
    }

    /**
     */
    public void getStatus(alluxio.grpc.GetStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetStatusPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetStatusMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class FileSystemMasterServiceBlockingStub extends io.grpc.stub.AbstractStub<FileSystemMasterServiceBlockingStub> {
    private FileSystemMasterServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public alluxio.grpc.GetStatusPResponse getStatus(alluxio.grpc.GetStatusPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetStatusMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class FileSystemMasterServiceFutureStub extends io.grpc.stub.AbstractStub<FileSystemMasterServiceFutureStub> {
    private FileSystemMasterServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetStatusPResponse> getStatus(
        alluxio.grpc.GetStatusPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetStatusMethodHelper(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_STATUS = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final FileSystemMasterServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(FileSystemMasterServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_STATUS:
          serviceImpl.getStatus((alluxio.grpc.GetStatusPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetStatusPResponse>) responseObserver);
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

  private static abstract class FileSystemMasterServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    FileSystemMasterServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("FileSystemMasterService");
    }
  }

  private static final class FileSystemMasterServiceFileDescriptorSupplier
      extends FileSystemMasterServiceBaseDescriptorSupplier {
    FileSystemMasterServiceFileDescriptorSupplier() {}
  }

  private static final class FileSystemMasterServiceMethodDescriptorSupplier
      extends FileSystemMasterServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    FileSystemMasterServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (FileSystemMasterServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new FileSystemMasterServiceFileDescriptorSupplier())
              .addMethod(getGetStatusMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
