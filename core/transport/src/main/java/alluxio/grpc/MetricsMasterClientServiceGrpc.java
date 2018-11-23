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
 * This interface contains metrics master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.10.1)",
    comments = "Source: metric_master.proto")
public final class MetricsMasterClientServiceGrpc {

  private MetricsMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.MetricsMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getMetricsHeartbeatMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.MetricsHeartbeatPRequest,
      alluxio.grpc.MetricsHeartbeatPResponse> METHOD_METRICS_HEARTBEAT = getMetricsHeartbeatMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.MetricsHeartbeatPRequest,
      alluxio.grpc.MetricsHeartbeatPResponse> getMetricsHeartbeatMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.MetricsHeartbeatPRequest,
      alluxio.grpc.MetricsHeartbeatPResponse> getMetricsHeartbeatMethod() {
    return getMetricsHeartbeatMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.MetricsHeartbeatPRequest,
      alluxio.grpc.MetricsHeartbeatPResponse> getMetricsHeartbeatMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.MetricsHeartbeatPRequest, alluxio.grpc.MetricsHeartbeatPResponse> getMetricsHeartbeatMethod;
    if ((getMetricsHeartbeatMethod = MetricsMasterClientServiceGrpc.getMetricsHeartbeatMethod) == null) {
      synchronized (MetricsMasterClientServiceGrpc.class) {
        if ((getMetricsHeartbeatMethod = MetricsMasterClientServiceGrpc.getMetricsHeartbeatMethod) == null) {
          MetricsMasterClientServiceGrpc.getMetricsHeartbeatMethod = getMetricsHeartbeatMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.MetricsHeartbeatPRequest, alluxio.grpc.MetricsHeartbeatPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.MetricsMasterClientService", "MetricsHeartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.MetricsHeartbeatPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.MetricsHeartbeatPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new MetricsMasterClientServiceMethodDescriptorSupplier("MetricsHeartbeat"))
                  .build();
          }
        }
     }
     return getMetricsHeartbeatMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MetricsMasterClientServiceStub newStub(io.grpc.Channel channel) {
    return new MetricsMasterClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetricsMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new MetricsMasterClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MetricsMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new MetricsMasterClientServiceFutureStub(channel);
  }

  /**
   * <pre>
   **
   * This interface contains metrics master service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class MetricsMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Periodic metrics master client heartbeat.
     * </pre>
     */
    public void metricsHeartbeat(alluxio.grpc.MetricsHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MetricsHeartbeatPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getMetricsHeartbeatMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getMetricsHeartbeatMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.MetricsHeartbeatPRequest,
                alluxio.grpc.MetricsHeartbeatPResponse>(
                  this, METHODID_METRICS_HEARTBEAT)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains metrics master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetricsMasterClientServiceStub extends io.grpc.stub.AbstractStub<MetricsMasterClientServiceStub> {
    private MetricsMasterClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetricsMasterClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetricsMasterClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetricsMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Periodic metrics master client heartbeat.
     * </pre>
     */
    public void metricsHeartbeat(alluxio.grpc.MetricsHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MetricsHeartbeatPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getMetricsHeartbeatMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains metrics master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetricsMasterClientServiceBlockingStub extends io.grpc.stub.AbstractStub<MetricsMasterClientServiceBlockingStub> {
    private MetricsMasterClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetricsMasterClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetricsMasterClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetricsMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Periodic metrics master client heartbeat.
     * </pre>
     */
    public alluxio.grpc.MetricsHeartbeatPResponse metricsHeartbeat(alluxio.grpc.MetricsHeartbeatPRequest request) {
      return blockingUnaryCall(
          getChannel(), getMetricsHeartbeatMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains metrics master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetricsMasterClientServiceFutureStub extends io.grpc.stub.AbstractStub<MetricsMasterClientServiceFutureStub> {
    private MetricsMasterClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetricsMasterClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetricsMasterClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetricsMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Periodic metrics master client heartbeat.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.MetricsHeartbeatPResponse> metricsHeartbeat(
        alluxio.grpc.MetricsHeartbeatPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getMetricsHeartbeatMethodHelper(), getCallOptions()), request);
    }
  }

  private static final int METHODID_METRICS_HEARTBEAT = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MetricsMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(MetricsMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_METRICS_HEARTBEAT:
          serviceImpl.metricsHeartbeat((alluxio.grpc.MetricsHeartbeatPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.MetricsHeartbeatPResponse>) responseObserver);
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

  private static abstract class MetricsMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MetricsMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.MetricMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("MetricsMasterClientService");
    }
  }

  private static final class MetricsMasterClientServiceFileDescriptorSupplier
      extends MetricsMasterClientServiceBaseDescriptorSupplier {
    MetricsMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class MetricsMasterClientServiceMethodDescriptorSupplier
      extends MetricsMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    MetricsMasterClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (MetricsMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MetricsMasterClientServiceFileDescriptorSupplier())
              .addMethod(getMetricsHeartbeatMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
