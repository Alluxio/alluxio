package alluxio.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 **
 * This interface contains meta master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.37.0)",
    comments = "Source: grpc/meta_master.proto")
public final class MetaMasterClientServiceGrpc {

  private MetaMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.meta.MetaMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.BackupPRequest,
      alluxio.grpc.BackupPStatus> getBackupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Backup",
      requestType = alluxio.grpc.BackupPRequest.class,
      responseType = alluxio.grpc.BackupPStatus.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.BackupPRequest,
      alluxio.grpc.BackupPStatus> getBackupMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.BackupPRequest, alluxio.grpc.BackupPStatus> getBackupMethod;
    if ((getBackupMethod = MetaMasterClientServiceGrpc.getBackupMethod) == null) {
      synchronized (MetaMasterClientServiceGrpc.class) {
        if ((getBackupMethod = MetaMasterClientServiceGrpc.getBackupMethod) == null) {
          MetaMasterClientServiceGrpc.getBackupMethod = getBackupMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.BackupPRequest, alluxio.grpc.BackupPStatus>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Backup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.BackupPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.BackupPStatus.getDefaultInstance()))
              .setSchemaDescriptor(new MetaMasterClientServiceMethodDescriptorSupplier("Backup"))
              .build();
        }
      }
    }
    return getBackupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.BackupStatusPRequest,
      alluxio.grpc.BackupPStatus> getGetBackupStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetBackupStatus",
      requestType = alluxio.grpc.BackupStatusPRequest.class,
      responseType = alluxio.grpc.BackupPStatus.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.BackupStatusPRequest,
      alluxio.grpc.BackupPStatus> getGetBackupStatusMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.BackupStatusPRequest, alluxio.grpc.BackupPStatus> getGetBackupStatusMethod;
    if ((getGetBackupStatusMethod = MetaMasterClientServiceGrpc.getGetBackupStatusMethod) == null) {
      synchronized (MetaMasterClientServiceGrpc.class) {
        if ((getGetBackupStatusMethod = MetaMasterClientServiceGrpc.getGetBackupStatusMethod) == null) {
          MetaMasterClientServiceGrpc.getGetBackupStatusMethod = getGetBackupStatusMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.BackupStatusPRequest, alluxio.grpc.BackupPStatus>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetBackupStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.BackupStatusPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.BackupPStatus.getDefaultInstance()))
              .setSchemaDescriptor(new MetaMasterClientServiceMethodDescriptorSupplier("GetBackupStatus"))
              .build();
        }
      }
    }
    return getGetBackupStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetConfigReportPOptions,
      alluxio.grpc.GetConfigReportPResponse> getGetConfigReportMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetConfigReport",
      requestType = alluxio.grpc.GetConfigReportPOptions.class,
      responseType = alluxio.grpc.GetConfigReportPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetConfigReportPOptions,
      alluxio.grpc.GetConfigReportPResponse> getGetConfigReportMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetConfigReportPOptions, alluxio.grpc.GetConfigReportPResponse> getGetConfigReportMethod;
    if ((getGetConfigReportMethod = MetaMasterClientServiceGrpc.getGetConfigReportMethod) == null) {
      synchronized (MetaMasterClientServiceGrpc.class) {
        if ((getGetConfigReportMethod = MetaMasterClientServiceGrpc.getGetConfigReportMethod) == null) {
          MetaMasterClientServiceGrpc.getGetConfigReportMethod = getGetConfigReportMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetConfigReportPOptions, alluxio.grpc.GetConfigReportPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetConfigReport"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetConfigReportPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetConfigReportPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetaMasterClientServiceMethodDescriptorSupplier("GetConfigReport"))
              .build();
        }
      }
    }
    return getGetConfigReportMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetMasterInfoPOptions,
      alluxio.grpc.GetMasterInfoPResponse> getGetMasterInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetMasterInfo",
      requestType = alluxio.grpc.GetMasterInfoPOptions.class,
      responseType = alluxio.grpc.GetMasterInfoPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetMasterInfoPOptions,
      alluxio.grpc.GetMasterInfoPResponse> getGetMasterInfoMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetMasterInfoPOptions, alluxio.grpc.GetMasterInfoPResponse> getGetMasterInfoMethod;
    if ((getGetMasterInfoMethod = MetaMasterClientServiceGrpc.getGetMasterInfoMethod) == null) {
      synchronized (MetaMasterClientServiceGrpc.class) {
        if ((getGetMasterInfoMethod = MetaMasterClientServiceGrpc.getGetMasterInfoMethod) == null) {
          MetaMasterClientServiceGrpc.getGetMasterInfoMethod = getGetMasterInfoMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetMasterInfoPOptions, alluxio.grpc.GetMasterInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetMasterInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetMasterInfoPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetMasterInfoPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetaMasterClientServiceMethodDescriptorSupplier("GetMasterInfo"))
              .build();
        }
      }
    }
    return getGetMasterInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CheckpointPOptions,
      alluxio.grpc.CheckpointPResponse> getCheckpointMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Checkpoint",
      requestType = alluxio.grpc.CheckpointPOptions.class,
      responseType = alluxio.grpc.CheckpointPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CheckpointPOptions,
      alluxio.grpc.CheckpointPResponse> getCheckpointMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CheckpointPOptions, alluxio.grpc.CheckpointPResponse> getCheckpointMethod;
    if ((getCheckpointMethod = MetaMasterClientServiceGrpc.getCheckpointMethod) == null) {
      synchronized (MetaMasterClientServiceGrpc.class) {
        if ((getCheckpointMethod = MetaMasterClientServiceGrpc.getCheckpointMethod) == null) {
          MetaMasterClientServiceGrpc.getCheckpointMethod = getCheckpointMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.CheckpointPOptions, alluxio.grpc.CheckpointPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Checkpoint"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CheckpointPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CheckpointPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetaMasterClientServiceMethodDescriptorSupplier("Checkpoint"))
              .build();
        }
      }
    }
    return getCheckpointMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MetaMasterClientServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetaMasterClientServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetaMasterClientServiceStub>() {
        @java.lang.Override
        public MetaMasterClientServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetaMasterClientServiceStub(channel, callOptions);
        }
      };
    return MetaMasterClientServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetaMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetaMasterClientServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetaMasterClientServiceBlockingStub>() {
        @java.lang.Override
        public MetaMasterClientServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetaMasterClientServiceBlockingStub(channel, callOptions);
        }
      };
    return MetaMasterClientServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MetaMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetaMasterClientServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetaMasterClientServiceFutureStub>() {
        @java.lang.Override
        public MetaMasterClientServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetaMasterClientServiceFutureStub(channel, callOptions);
        }
      };
    return MetaMasterClientServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class MetaMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Backs up the Alluxio master to the specified URI
     * </pre>
     */
    public void backup(alluxio.grpc.BackupPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.BackupPStatus> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getBackupMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns status of the latest backup.
     * </pre>
     */
    public void getBackupStatus(alluxio.grpc.BackupStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.BackupPStatus> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetBackupStatusMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns server-side configuration report.
     * </pre>
     */
    public void getConfigReport(alluxio.grpc.GetConfigReportPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigReportPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetConfigReportMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns information about the master.
     * </pre>
     */
    public void getMasterInfo(alluxio.grpc.GetMasterInfoPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMasterInfoPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMasterInfoMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a checkpoint in the primary master journal system.
     * </pre>
     */
    public void checkpoint(alluxio.grpc.CheckpointPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CheckpointPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCheckpointMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getBackupMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.BackupPRequest,
                alluxio.grpc.BackupPStatus>(
                  this, METHODID_BACKUP)))
          .addMethod(
            getGetBackupStatusMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.BackupStatusPRequest,
                alluxio.grpc.BackupPStatus>(
                  this, METHODID_GET_BACKUP_STATUS)))
          .addMethod(
            getGetConfigReportMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetConfigReportPOptions,
                alluxio.grpc.GetConfigReportPResponse>(
                  this, METHODID_GET_CONFIG_REPORT)))
          .addMethod(
            getGetMasterInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetMasterInfoPOptions,
                alluxio.grpc.GetMasterInfoPResponse>(
                  this, METHODID_GET_MASTER_INFO)))
          .addMethod(
            getCheckpointMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CheckpointPOptions,
                alluxio.grpc.CheckpointPResponse>(
                  this, METHODID_CHECKPOINT)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetaMasterClientServiceStub extends io.grpc.stub.AbstractAsyncStub<MetaMasterClientServiceStub> {
    private MetaMasterClientServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterClientServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetaMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Backs up the Alluxio master to the specified URI
     * </pre>
     */
    public void backup(alluxio.grpc.BackupPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.BackupPStatus> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getBackupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns status of the latest backup.
     * </pre>
     */
    public void getBackupStatus(alluxio.grpc.BackupStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.BackupPStatus> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetBackupStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns server-side configuration report.
     * </pre>
     */
    public void getConfigReport(alluxio.grpc.GetConfigReportPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigReportPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetConfigReportMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns information about the master.
     * </pre>
     */
    public void getMasterInfo(alluxio.grpc.GetMasterInfoPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMasterInfoPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMasterInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a checkpoint in the primary master journal system.
     * </pre>
     */
    public void checkpoint(alluxio.grpc.CheckpointPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CheckpointPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCheckpointMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetaMasterClientServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<MetaMasterClientServiceBlockingStub> {
    private MetaMasterClientServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterClientServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetaMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Backs up the Alluxio master to the specified URI
     * </pre>
     */
    public alluxio.grpc.BackupPStatus backup(alluxio.grpc.BackupPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getBackupMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns status of the latest backup.
     * </pre>
     */
    public alluxio.grpc.BackupPStatus getBackupStatus(alluxio.grpc.BackupStatusPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetBackupStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns server-side configuration report.
     * </pre>
     */
    public alluxio.grpc.GetConfigReportPResponse getConfigReport(alluxio.grpc.GetConfigReportPOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetConfigReportMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns information about the master.
     * </pre>
     */
    public alluxio.grpc.GetMasterInfoPResponse getMasterInfo(alluxio.grpc.GetMasterInfoPOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMasterInfoMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Creates a checkpoint in the primary master journal system.
     * </pre>
     */
    public alluxio.grpc.CheckpointPResponse checkpoint(alluxio.grpc.CheckpointPOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCheckpointMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetaMasterClientServiceFutureStub extends io.grpc.stub.AbstractFutureStub<MetaMasterClientServiceFutureStub> {
    private MetaMasterClientServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterClientServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetaMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Backs up the Alluxio master to the specified URI
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.BackupPStatus> backup(
        alluxio.grpc.BackupPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getBackupMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns status of the latest backup.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.BackupPStatus> getBackupStatus(
        alluxio.grpc.BackupStatusPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetBackupStatusMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns server-side configuration report.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetConfigReportPResponse> getConfigReport(
        alluxio.grpc.GetConfigReportPOptions request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetConfigReportMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns information about the master.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetMasterInfoPResponse> getMasterInfo(
        alluxio.grpc.GetMasterInfoPOptions request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMasterInfoMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Creates a checkpoint in the primary master journal system.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CheckpointPResponse> checkpoint(
        alluxio.grpc.CheckpointPOptions request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCheckpointMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_BACKUP = 0;
  private static final int METHODID_GET_BACKUP_STATUS = 1;
  private static final int METHODID_GET_CONFIG_REPORT = 2;
  private static final int METHODID_GET_MASTER_INFO = 3;
  private static final int METHODID_CHECKPOINT = 4;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MetaMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(MetaMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_BACKUP:
          serviceImpl.backup((alluxio.grpc.BackupPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.BackupPStatus>) responseObserver);
          break;
        case METHODID_GET_BACKUP_STATUS:
          serviceImpl.getBackupStatus((alluxio.grpc.BackupStatusPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.BackupPStatus>) responseObserver);
          break;
        case METHODID_GET_CONFIG_REPORT:
          serviceImpl.getConfigReport((alluxio.grpc.GetConfigReportPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigReportPResponse>) responseObserver);
          break;
        case METHODID_GET_MASTER_INFO:
          serviceImpl.getMasterInfo((alluxio.grpc.GetMasterInfoPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetMasterInfoPResponse>) responseObserver);
          break;
        case METHODID_CHECKPOINT:
          serviceImpl.checkpoint((alluxio.grpc.CheckpointPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CheckpointPResponse>) responseObserver);
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

  private static abstract class MetaMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MetaMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.MetaMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("MetaMasterClientService");
    }
  }

  private static final class MetaMasterClientServiceFileDescriptorSupplier
      extends MetaMasterClientServiceBaseDescriptorSupplier {
    MetaMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class MetaMasterClientServiceMethodDescriptorSupplier
      extends MetaMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    MetaMasterClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (MetaMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MetaMasterClientServiceFileDescriptorSupplier())
              .addMethod(getBackupMethod())
              .addMethod(getGetBackupStatusMethod())
              .addMethod(getGetConfigReportMethod())
              .addMethod(getGetMasterInfoMethod())
              .addMethod(getCheckpointMethod())
              .build();
        }
      }
    }
    return result;
  }
}
