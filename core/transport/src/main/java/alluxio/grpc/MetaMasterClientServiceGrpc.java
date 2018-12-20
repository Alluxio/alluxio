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
 * This interface contains meta master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.10.1)",
    comments = "Source: meta_master.proto")
public final class MetaMasterClientServiceGrpc {

  private MetaMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.MetaMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getBackupMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.BackupPOptions,
      alluxio.grpc.BackupPResponse> METHOD_BACKUP = getBackupMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.BackupPOptions,
      alluxio.grpc.BackupPResponse> getBackupMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.BackupPOptions,
      alluxio.grpc.BackupPResponse> getBackupMethod() {
    return getBackupMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.BackupPOptions,
      alluxio.grpc.BackupPResponse> getBackupMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.BackupPOptions, alluxio.grpc.BackupPResponse> getBackupMethod;
    if ((getBackupMethod = MetaMasterClientServiceGrpc.getBackupMethod) == null) {
      synchronized (MetaMasterClientServiceGrpc.class) {
        if ((getBackupMethod = MetaMasterClientServiceGrpc.getBackupMethod) == null) {
          MetaMasterClientServiceGrpc.getBackupMethod = getBackupMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.BackupPOptions, alluxio.grpc.BackupPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.MetaMasterClientService", "Backup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.BackupPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.BackupPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new MetaMasterClientServiceMethodDescriptorSupplier("Backup"))
                  .build();
          }
        }
     }
     return getBackupMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetConfigReportMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetConfigReportPOptions,
      alluxio.grpc.GetConfigReportPResponse> METHOD_GET_CONFIG_REPORT = getGetConfigReportMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetConfigReportPOptions,
      alluxio.grpc.GetConfigReportPResponse> getGetConfigReportMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetConfigReportPOptions,
      alluxio.grpc.GetConfigReportPResponse> getGetConfigReportMethod() {
    return getGetConfigReportMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetConfigReportPOptions,
      alluxio.grpc.GetConfigReportPResponse> getGetConfigReportMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetConfigReportPOptions, alluxio.grpc.GetConfigReportPResponse> getGetConfigReportMethod;
    if ((getGetConfigReportMethod = MetaMasterClientServiceGrpc.getGetConfigReportMethod) == null) {
      synchronized (MetaMasterClientServiceGrpc.class) {
        if ((getGetConfigReportMethod = MetaMasterClientServiceGrpc.getGetConfigReportMethod) == null) {
          MetaMasterClientServiceGrpc.getGetConfigReportMethod = getGetConfigReportMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetConfigReportPOptions, alluxio.grpc.GetConfigReportPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.MetaMasterClientService", "GetConfigReport"))
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
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetMasterInfoMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetMasterInfoPOptions,
      alluxio.grpc.GetMasterInfoPResponse> METHOD_GET_MASTER_INFO = getGetMasterInfoMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetMasterInfoPOptions,
      alluxio.grpc.GetMasterInfoPResponse> getGetMasterInfoMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetMasterInfoPOptions,
      alluxio.grpc.GetMasterInfoPResponse> getGetMasterInfoMethod() {
    return getGetMasterInfoMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetMasterInfoPOptions,
      alluxio.grpc.GetMasterInfoPResponse> getGetMasterInfoMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetMasterInfoPOptions, alluxio.grpc.GetMasterInfoPResponse> getGetMasterInfoMethod;
    if ((getGetMasterInfoMethod = MetaMasterClientServiceGrpc.getGetMasterInfoMethod) == null) {
      synchronized (MetaMasterClientServiceGrpc.class) {
        if ((getGetMasterInfoMethod = MetaMasterClientServiceGrpc.getGetMasterInfoMethod) == null) {
          MetaMasterClientServiceGrpc.getGetMasterInfoMethod = getGetMasterInfoMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetMasterInfoPOptions, alluxio.grpc.GetMasterInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.MetaMasterClientService", "GetMasterInfo"))
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
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetMetricsMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetMetricsPOptions,
      alluxio.grpc.GetMetricsPResponse> METHOD_GET_METRICS = getGetMetricsMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetMetricsPOptions,
      alluxio.grpc.GetMetricsPResponse> getGetMetricsMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetMetricsPOptions,
      alluxio.grpc.GetMetricsPResponse> getGetMetricsMethod() {
    return getGetMetricsMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetMetricsPOptions,
      alluxio.grpc.GetMetricsPResponse> getGetMetricsMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetMetricsPOptions, alluxio.grpc.GetMetricsPResponse> getGetMetricsMethod;
    if ((getGetMetricsMethod = MetaMasterClientServiceGrpc.getGetMetricsMethod) == null) {
      synchronized (MetaMasterClientServiceGrpc.class) {
        if ((getGetMetricsMethod = MetaMasterClientServiceGrpc.getGetMetricsMethod) == null) {
          MetaMasterClientServiceGrpc.getGetMetricsMethod = getGetMetricsMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetMetricsPOptions, alluxio.grpc.GetMetricsPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.MetaMasterClientService", "GetMetrics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetMetricsPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetMetricsPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new MetaMasterClientServiceMethodDescriptorSupplier("GetMetrics"))
                  .build();
          }
        }
     }
     return getGetMetricsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MetaMasterClientServiceStub newStub(io.grpc.Channel channel) {
    return new MetaMasterClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetaMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new MetaMasterClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MetaMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new MetaMasterClientServiceFutureStub(channel);
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
    public void backup(alluxio.grpc.BackupPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.BackupPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getBackupMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns server-side configuration report.
     * </pre>
     */
    public void getConfigReport(alluxio.grpc.GetConfigReportPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigReportPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetConfigReportMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns information about the master.
     * </pre>
     */
    public void getMasterInfo(alluxio.grpc.GetMasterInfoPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMasterInfoPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetMasterInfoMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a map of metrics property names and their values from Alluxio metrics system.
     * </pre>
     */
    public void getMetrics(alluxio.grpc.GetMetricsPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMetricsPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetMetricsMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getBackupMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.BackupPOptions,
                alluxio.grpc.BackupPResponse>(
                  this, METHODID_BACKUP)))
          .addMethod(
            getGetConfigReportMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetConfigReportPOptions,
                alluxio.grpc.GetConfigReportPResponse>(
                  this, METHODID_GET_CONFIG_REPORT)))
          .addMethod(
            getGetMasterInfoMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetMasterInfoPOptions,
                alluxio.grpc.GetMasterInfoPResponse>(
                  this, METHODID_GET_MASTER_INFO)))
          .addMethod(
            getGetMetricsMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetMetricsPOptions,
                alluxio.grpc.GetMetricsPResponse>(
                  this, METHODID_GET_METRICS)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetaMasterClientServiceStub extends io.grpc.stub.AbstractStub<MetaMasterClientServiceStub> {
    private MetaMasterClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaMasterClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Backs up the Alluxio master to the specified URI
     * </pre>
     */
    public void backup(alluxio.grpc.BackupPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.BackupPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getBackupMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns server-side configuration report.
     * </pre>
     */
    public void getConfigReport(alluxio.grpc.GetConfigReportPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigReportPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetConfigReportMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns information about the master.
     * </pre>
     */
    public void getMasterInfo(alluxio.grpc.GetMasterInfoPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMasterInfoPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetMasterInfoMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a map of metrics property names and their values from Alluxio metrics system.
     * </pre>
     */
    public void getMetrics(alluxio.grpc.GetMetricsPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMetricsPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetMetricsMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetaMasterClientServiceBlockingStub extends io.grpc.stub.AbstractStub<MetaMasterClientServiceBlockingStub> {
    private MetaMasterClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaMasterClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Backs up the Alluxio master to the specified URI
     * </pre>
     */
    public alluxio.grpc.BackupPResponse backup(alluxio.grpc.BackupPOptions request) {
      return blockingUnaryCall(
          getChannel(), getBackupMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns server-side configuration report.
     * </pre>
     */
    public alluxio.grpc.GetConfigReportPResponse getConfigReport(alluxio.grpc.GetConfigReportPOptions request) {
      return blockingUnaryCall(
          getChannel(), getGetConfigReportMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns information about the master.
     * </pre>
     */
    public alluxio.grpc.GetMasterInfoPResponse getMasterInfo(alluxio.grpc.GetMasterInfoPOptions request) {
      return blockingUnaryCall(
          getChannel(), getGetMasterInfoMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a map of metrics property names and their values from Alluxio metrics system.
     * </pre>
     */
    public alluxio.grpc.GetMetricsPResponse getMetrics(alluxio.grpc.GetMetricsPOptions request) {
      return blockingUnaryCall(
          getChannel(), getGetMetricsMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains meta master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetaMasterClientServiceFutureStub extends io.grpc.stub.AbstractStub<MetaMasterClientServiceFutureStub> {
    private MetaMasterClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaMasterClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaMasterClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Backs up the Alluxio master to the specified URI
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.BackupPResponse> backup(
        alluxio.grpc.BackupPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getBackupMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns server-side configuration report.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetConfigReportPResponse> getConfigReport(
        alluxio.grpc.GetConfigReportPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getGetConfigReportMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns information about the master.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetMasterInfoPResponse> getMasterInfo(
        alluxio.grpc.GetMasterInfoPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getGetMasterInfoMethodHelper(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a map of metrics property names and their values from Alluxio metrics system.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetMetricsPResponse> getMetrics(
        alluxio.grpc.GetMetricsPOptions request) {
      return futureUnaryCall(
          getChannel().newCall(getGetMetricsMethodHelper(), getCallOptions()), request);
    }
  }

  private static final int METHODID_BACKUP = 0;
  private static final int METHODID_GET_CONFIG_REPORT = 1;
  private static final int METHODID_GET_MASTER_INFO = 2;
  private static final int METHODID_GET_METRICS = 3;

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
          serviceImpl.backup((alluxio.grpc.BackupPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.BackupPResponse>) responseObserver);
          break;
        case METHODID_GET_CONFIG_REPORT:
          serviceImpl.getConfigReport((alluxio.grpc.GetConfigReportPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetConfigReportPResponse>) responseObserver);
          break;
        case METHODID_GET_MASTER_INFO:
          serviceImpl.getMasterInfo((alluxio.grpc.GetMasterInfoPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetMasterInfoPResponse>) responseObserver);
          break;
        case METHODID_GET_METRICS:
          serviceImpl.getMetrics((alluxio.grpc.GetMetricsPOptions) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetMetricsPResponse>) responseObserver);
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
              .addMethod(getBackupMethodHelper())
              .addMethod(getGetConfigReportMethodHelper())
              .addMethod(getGetMasterInfoMethodHelper())
              .addMethod(getGetMetricsMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
