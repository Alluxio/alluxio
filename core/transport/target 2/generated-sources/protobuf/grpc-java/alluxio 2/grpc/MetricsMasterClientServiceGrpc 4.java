package alluxio.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 **
 * This interface contains metrics master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.37.0)",
    comments = "Source: grpc/metric_master.proto")
public final class MetricsMasterClientServiceGrpc {

  private MetricsMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.metric.MetricsMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ClearMetricsPRequest,
      alluxio.grpc.ClearMetricsPResponse> getClearMetricsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ClearMetrics",
      requestType = alluxio.grpc.ClearMetricsPRequest.class,
      responseType = alluxio.grpc.ClearMetricsPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.ClearMetricsPRequest,
      alluxio.grpc.ClearMetricsPResponse> getClearMetricsMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.ClearMetricsPRequest, alluxio.grpc.ClearMetricsPResponse> getClearMetricsMethod;
    if ((getClearMetricsMethod = MetricsMasterClientServiceGrpc.getClearMetricsMethod) == null) {
      synchronized (MetricsMasterClientServiceGrpc.class) {
        if ((getClearMetricsMethod = MetricsMasterClientServiceGrpc.getClearMetricsMethod) == null) {
          MetricsMasterClientServiceGrpc.getClearMetricsMethod = getClearMetricsMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.ClearMetricsPRequest, alluxio.grpc.ClearMetricsPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ClearMetrics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ClearMetricsPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ClearMetricsPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetricsMasterClientServiceMethodDescriptorSupplier("ClearMetrics"))
              .build();
        }
      }
    }
    return getClearMetricsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.MetricsHeartbeatPRequest,
      alluxio.grpc.MetricsHeartbeatPResponse> getMetricsHeartbeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MetricsHeartbeat",
      requestType = alluxio.grpc.MetricsHeartbeatPRequest.class,
      responseType = alluxio.grpc.MetricsHeartbeatPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.MetricsHeartbeatPRequest,
      alluxio.grpc.MetricsHeartbeatPResponse> getMetricsHeartbeatMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.MetricsHeartbeatPRequest, alluxio.grpc.MetricsHeartbeatPResponse> getMetricsHeartbeatMethod;
    if ((getMetricsHeartbeatMethod = MetricsMasterClientServiceGrpc.getMetricsHeartbeatMethod) == null) {
      synchronized (MetricsMasterClientServiceGrpc.class) {
        if ((getMetricsHeartbeatMethod = MetricsMasterClientServiceGrpc.getMetricsHeartbeatMethod) == null) {
          MetricsMasterClientServiceGrpc.getMetricsHeartbeatMethod = getMetricsHeartbeatMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.MetricsHeartbeatPRequest, alluxio.grpc.MetricsHeartbeatPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MetricsHeartbeat"))
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

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetMetricsPOptions,
      alluxio.grpc.GetMetricsPResponse> getGetMetricsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetMetrics",
      requestType = alluxio.grpc.GetMetricsPOptions.class,
      responseType = alluxio.grpc.GetMetricsPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetMetricsPOptions,
      alluxio.grpc.GetMetricsPResponse> getGetMetricsMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetMetricsPOptions, alluxio.grpc.GetMetricsPResponse> getGetMetricsMethod;
    if ((getGetMetricsMethod = MetricsMasterClientServiceGrpc.getGetMetricsMethod) == null) {
      synchronized (MetricsMasterClientServiceGrpc.class) {
        if ((getGetMetricsMethod = MetricsMasterClientServiceGrpc.getGetMetricsMethod) == null) {
          MetricsMasterClientServiceGrpc.getGetMetricsMethod = getGetMetricsMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetMetricsPOptions, alluxio.grpc.GetMetricsPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetMetrics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetMetricsPOptions.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetMetricsPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetricsMasterClientServiceMethodDescriptorSupplier("GetMetrics"))
              .build();
        }
      }
    }
    return getGetMetricsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MetricsMasterClientServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetricsMasterClientServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetricsMasterClientServiceStub>() {
        @java.lang.Override
        public MetricsMasterClientServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetricsMasterClientServiceStub(channel, callOptions);
        }
      };
    return MetricsMasterClientServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetricsMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetricsMasterClientServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetricsMasterClientServiceBlockingStub>() {
        @java.lang.Override
        public MetricsMasterClientServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetricsMasterClientServiceBlockingStub(channel, callOptions);
        }
      };
    return MetricsMasterClientServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MetricsMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetricsMasterClientServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetricsMasterClientServiceFutureStub>() {
        @java.lang.Override
        public MetricsMasterClientServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetricsMasterClientServiceFutureStub(channel, callOptions);
        }
      };
    return MetricsMasterClientServiceFutureStub.newStub(factory, channel);
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
     * Clears the metrics in the cluster.
     * </pre>
     */
    public void clearMetrics(alluxio.grpc.ClearMetricsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ClearMetricsPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getClearMetricsMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Periodic metrics master client heartbeat.
     * </pre>
     */
    public void metricsHeartbeat(alluxio.grpc.MetricsHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MetricsHeartbeatPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMetricsHeartbeatMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a map of metrics property names and their values from Alluxio metrics system.
     * </pre>
     */
    public void getMetrics(alluxio.grpc.GetMetricsPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMetricsPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMetricsMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getClearMetricsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.ClearMetricsPRequest,
                alluxio.grpc.ClearMetricsPResponse>(
                  this, METHODID_CLEAR_METRICS)))
          .addMethod(
            getMetricsHeartbeatMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.MetricsHeartbeatPRequest,
                alluxio.grpc.MetricsHeartbeatPResponse>(
                  this, METHODID_METRICS_HEARTBEAT)))
          .addMethod(
            getGetMetricsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
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
   * This interface contains metrics master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetricsMasterClientServiceStub extends io.grpc.stub.AbstractAsyncStub<MetricsMasterClientServiceStub> {
    private MetricsMasterClientServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetricsMasterClientServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetricsMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Clears the metrics in the cluster.
     * </pre>
     */
    public void clearMetrics(alluxio.grpc.ClearMetricsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ClearMetricsPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getClearMetricsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Periodic metrics master client heartbeat.
     * </pre>
     */
    public void metricsHeartbeat(alluxio.grpc.MetricsHeartbeatPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MetricsHeartbeatPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMetricsHeartbeatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a map of metrics property names and their values from Alluxio metrics system.
     * </pre>
     */
    public void getMetrics(alluxio.grpc.GetMetricsPOptions request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMetricsPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMetricsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains metrics master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetricsMasterClientServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<MetricsMasterClientServiceBlockingStub> {
    private MetricsMasterClientServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetricsMasterClientServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetricsMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Clears the metrics in the cluster.
     * </pre>
     */
    public alluxio.grpc.ClearMetricsPResponse clearMetrics(alluxio.grpc.ClearMetricsPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getClearMetricsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Periodic metrics master client heartbeat.
     * </pre>
     */
    public alluxio.grpc.MetricsHeartbeatPResponse metricsHeartbeat(alluxio.grpc.MetricsHeartbeatPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMetricsHeartbeatMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a map of metrics property names and their values from Alluxio metrics system.
     * </pre>
     */
    public alluxio.grpc.GetMetricsPResponse getMetrics(alluxio.grpc.GetMetricsPOptions request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMetricsMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains metrics master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class MetricsMasterClientServiceFutureStub extends io.grpc.stub.AbstractFutureStub<MetricsMasterClientServiceFutureStub> {
    private MetricsMasterClientServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetricsMasterClientServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetricsMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Clears the metrics in the cluster.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.ClearMetricsPResponse> clearMetrics(
        alluxio.grpc.ClearMetricsPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getClearMetricsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Periodic metrics master client heartbeat.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.MetricsHeartbeatPResponse> metricsHeartbeat(
        alluxio.grpc.MetricsHeartbeatPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMetricsHeartbeatMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a map of metrics property names and their values from Alluxio metrics system.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetMetricsPResponse> getMetrics(
        alluxio.grpc.GetMetricsPOptions request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMetricsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CLEAR_METRICS = 0;
  private static final int METHODID_METRICS_HEARTBEAT = 1;
  private static final int METHODID_GET_METRICS = 2;

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
        case METHODID_CLEAR_METRICS:
          serviceImpl.clearMetrics((alluxio.grpc.ClearMetricsPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.ClearMetricsPResponse>) responseObserver);
          break;
        case METHODID_METRICS_HEARTBEAT:
          serviceImpl.metricsHeartbeat((alluxio.grpc.MetricsHeartbeatPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.MetricsHeartbeatPResponse>) responseObserver);
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
              .addMethod(getClearMetricsMethod())
              .addMethod(getMetricsHeartbeatMethod())
              .addMethod(getGetMetricsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
