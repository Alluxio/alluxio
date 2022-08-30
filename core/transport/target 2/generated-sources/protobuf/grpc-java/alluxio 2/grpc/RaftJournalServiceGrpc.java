package alluxio.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 **
 * This interface contains raft service endpoints for Alluxio masters.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.37.0)",
    comments = "Source: grpc/raft_journal.proto")
public final class RaftJournalServiceGrpc {

  private RaftJournalServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.meta.RaftJournalService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.UploadSnapshotPRequest,
      alluxio.grpc.UploadSnapshotPResponse> getUploadSnapshotMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UploadSnapshot",
      requestType = alluxio.grpc.UploadSnapshotPRequest.class,
      responseType = alluxio.grpc.UploadSnapshotPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<alluxio.grpc.UploadSnapshotPRequest,
      alluxio.grpc.UploadSnapshotPResponse> getUploadSnapshotMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.UploadSnapshotPRequest, alluxio.grpc.UploadSnapshotPResponse> getUploadSnapshotMethod;
    if ((getUploadSnapshotMethod = RaftJournalServiceGrpc.getUploadSnapshotMethod) == null) {
      synchronized (RaftJournalServiceGrpc.class) {
        if ((getUploadSnapshotMethod = RaftJournalServiceGrpc.getUploadSnapshotMethod) == null) {
          RaftJournalServiceGrpc.getUploadSnapshotMethod = getUploadSnapshotMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.UploadSnapshotPRequest, alluxio.grpc.UploadSnapshotPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UploadSnapshot"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UploadSnapshotPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UploadSnapshotPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RaftJournalServiceMethodDescriptorSupplier("UploadSnapshot"))
              .build();
        }
      }
    }
    return getUploadSnapshotMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.DownloadSnapshotPRequest,
      alluxio.grpc.DownloadSnapshotPResponse> getDownloadSnapshotMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DownloadSnapshot",
      requestType = alluxio.grpc.DownloadSnapshotPRequest.class,
      responseType = alluxio.grpc.DownloadSnapshotPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<alluxio.grpc.DownloadSnapshotPRequest,
      alluxio.grpc.DownloadSnapshotPResponse> getDownloadSnapshotMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.DownloadSnapshotPRequest, alluxio.grpc.DownloadSnapshotPResponse> getDownloadSnapshotMethod;
    if ((getDownloadSnapshotMethod = RaftJournalServiceGrpc.getDownloadSnapshotMethod) == null) {
      synchronized (RaftJournalServiceGrpc.class) {
        if ((getDownloadSnapshotMethod = RaftJournalServiceGrpc.getDownloadSnapshotMethod) == null) {
          RaftJournalServiceGrpc.getDownloadSnapshotMethod = getDownloadSnapshotMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.DownloadSnapshotPRequest, alluxio.grpc.DownloadSnapshotPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DownloadSnapshot"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.DownloadSnapshotPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.DownloadSnapshotPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RaftJournalServiceMethodDescriptorSupplier("DownloadSnapshot"))
              .build();
        }
      }
    }
    return getDownloadSnapshotMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RaftJournalServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftJournalServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftJournalServiceStub>() {
        @java.lang.Override
        public RaftJournalServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftJournalServiceStub(channel, callOptions);
        }
      };
    return RaftJournalServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RaftJournalServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftJournalServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftJournalServiceBlockingStub>() {
        @java.lang.Override
        public RaftJournalServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftJournalServiceBlockingStub(channel, callOptions);
        }
      };
    return RaftJournalServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RaftJournalServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftJournalServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftJournalServiceFutureStub>() {
        @java.lang.Override
        public RaftJournalServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftJournalServiceFutureStub(channel, callOptions);
        }
      };
    return RaftJournalServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   **
   * This interface contains raft service endpoints for Alluxio masters.
   * </pre>
   */
  public static abstract class RaftJournalServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Uploads a snapshot to primary master.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.UploadSnapshotPRequest> uploadSnapshot(
        io.grpc.stub.StreamObserver<alluxio.grpc.UploadSnapshotPResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getUploadSnapshotMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Downloads a snapshot from primary master.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.DownloadSnapshotPRequest> downloadSnapshot(
        io.grpc.stub.StreamObserver<alluxio.grpc.DownloadSnapshotPResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getDownloadSnapshotMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getUploadSnapshotMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                alluxio.grpc.UploadSnapshotPRequest,
                alluxio.grpc.UploadSnapshotPResponse>(
                  this, METHODID_UPLOAD_SNAPSHOT)))
          .addMethod(
            getDownloadSnapshotMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                alluxio.grpc.DownloadSnapshotPRequest,
                alluxio.grpc.DownloadSnapshotPResponse>(
                  this, METHODID_DOWNLOAD_SNAPSHOT)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains raft service endpoints for Alluxio masters.
   * </pre>
   */
  public static final class RaftJournalServiceStub extends io.grpc.stub.AbstractAsyncStub<RaftJournalServiceStub> {
    private RaftJournalServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftJournalServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftJournalServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Uploads a snapshot to primary master.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.UploadSnapshotPRequest> uploadSnapshot(
        io.grpc.stub.StreamObserver<alluxio.grpc.UploadSnapshotPResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getUploadSnapshotMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     **
     * Downloads a snapshot from primary master.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.DownloadSnapshotPRequest> downloadSnapshot(
        io.grpc.stub.StreamObserver<alluxio.grpc.DownloadSnapshotPResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getDownloadSnapshotMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains raft service endpoints for Alluxio masters.
   * </pre>
   */
  public static final class RaftJournalServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<RaftJournalServiceBlockingStub> {
    private RaftJournalServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftJournalServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftJournalServiceBlockingStub(channel, callOptions);
    }
  }

  /**
   * <pre>
   **
   * This interface contains raft service endpoints for Alluxio masters.
   * </pre>
   */
  public static final class RaftJournalServiceFutureStub extends io.grpc.stub.AbstractFutureStub<RaftJournalServiceFutureStub> {
    private RaftJournalServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftJournalServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftJournalServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_UPLOAD_SNAPSHOT = 0;
  private static final int METHODID_DOWNLOAD_SNAPSHOT = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RaftJournalServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(RaftJournalServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_UPLOAD_SNAPSHOT:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.uploadSnapshot(
              (io.grpc.stub.StreamObserver<alluxio.grpc.UploadSnapshotPResponse>) responseObserver);
        case METHODID_DOWNLOAD_SNAPSHOT:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.downloadSnapshot(
              (io.grpc.stub.StreamObserver<alluxio.grpc.DownloadSnapshotPResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class RaftJournalServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RaftJournalServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.RaftJournalProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RaftJournalService");
    }
  }

  private static final class RaftJournalServiceFileDescriptorSupplier
      extends RaftJournalServiceBaseDescriptorSupplier {
    RaftJournalServiceFileDescriptorSupplier() {}
  }

  private static final class RaftJournalServiceMethodDescriptorSupplier
      extends RaftJournalServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    RaftJournalServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (RaftJournalServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RaftJournalServiceFileDescriptorSupplier())
              .addMethod(getUploadSnapshotMethod())
              .addMethod(getDownloadSnapshotMethod())
              .build();
        }
      }
    }
    return result;
  }
}
