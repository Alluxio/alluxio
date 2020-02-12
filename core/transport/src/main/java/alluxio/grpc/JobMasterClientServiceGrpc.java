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
 * This interface contains job master service endpoints for job service clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.27.0)",
    comments = "Source: grpc/job_master.proto")
public final class JobMasterClientServiceGrpc {

  private JobMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.job.JobMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CancelPRequest,
      alluxio.grpc.CancelPResponse> getCancelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Cancel",
      requestType = alluxio.grpc.CancelPRequest.class,
      responseType = alluxio.grpc.CancelPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CancelPRequest,
      alluxio.grpc.CancelPResponse> getCancelMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CancelPRequest, alluxio.grpc.CancelPResponse> getCancelMethod;
    if ((getCancelMethod = JobMasterClientServiceGrpc.getCancelMethod) == null) {
      synchronized (JobMasterClientServiceGrpc.class) {
        if ((getCancelMethod = JobMasterClientServiceGrpc.getCancelMethod) == null) {
          JobMasterClientServiceGrpc.getCancelMethod = getCancelMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.CancelPRequest, alluxio.grpc.CancelPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Cancel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CancelPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CancelPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobMasterClientServiceMethodDescriptorSupplier("Cancel"))
              .build();
        }
      }
    }
    return getCancelMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetJobStatusPRequest,
      alluxio.grpc.GetJobStatusPResponse> getGetJobStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetJobStatus",
      requestType = alluxio.grpc.GetJobStatusPRequest.class,
      responseType = alluxio.grpc.GetJobStatusPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetJobStatusPRequest,
      alluxio.grpc.GetJobStatusPResponse> getGetJobStatusMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetJobStatusPRequest, alluxio.grpc.GetJobStatusPResponse> getGetJobStatusMethod;
    if ((getGetJobStatusMethod = JobMasterClientServiceGrpc.getGetJobStatusMethod) == null) {
      synchronized (JobMasterClientServiceGrpc.class) {
        if ((getGetJobStatusMethod = JobMasterClientServiceGrpc.getGetJobStatusMethod) == null) {
          JobMasterClientServiceGrpc.getGetJobStatusMethod = getGetJobStatusMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetJobStatusPRequest, alluxio.grpc.GetJobStatusPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetJobStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetJobStatusPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetJobStatusPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobMasterClientServiceMethodDescriptorSupplier("GetJobStatus"))
              .build();
        }
      }
    }
    return getGetJobStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetJobServiceSummaryPRequest,
      alluxio.grpc.GetJobServiceSummaryPResponse> getGetJobServiceSummaryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetJobServiceSummary",
      requestType = alluxio.grpc.GetJobServiceSummaryPRequest.class,
      responseType = alluxio.grpc.GetJobServiceSummaryPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetJobServiceSummaryPRequest,
      alluxio.grpc.GetJobServiceSummaryPResponse> getGetJobServiceSummaryMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetJobServiceSummaryPRequest, alluxio.grpc.GetJobServiceSummaryPResponse> getGetJobServiceSummaryMethod;
    if ((getGetJobServiceSummaryMethod = JobMasterClientServiceGrpc.getGetJobServiceSummaryMethod) == null) {
      synchronized (JobMasterClientServiceGrpc.class) {
        if ((getGetJobServiceSummaryMethod = JobMasterClientServiceGrpc.getGetJobServiceSummaryMethod) == null) {
          JobMasterClientServiceGrpc.getGetJobServiceSummaryMethod = getGetJobServiceSummaryMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetJobServiceSummaryPRequest, alluxio.grpc.GetJobServiceSummaryPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetJobServiceSummary"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetJobServiceSummaryPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetJobServiceSummaryPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobMasterClientServiceMethodDescriptorSupplier("GetJobServiceSummary"))
              .build();
        }
      }
    }
    return getGetJobServiceSummaryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ListAllPRequest,
      alluxio.grpc.ListAllPResponse> getListAllMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListAll",
      requestType = alluxio.grpc.ListAllPRequest.class,
      responseType = alluxio.grpc.ListAllPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.ListAllPRequest,
      alluxio.grpc.ListAllPResponse> getListAllMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.ListAllPRequest, alluxio.grpc.ListAllPResponse> getListAllMethod;
    if ((getListAllMethod = JobMasterClientServiceGrpc.getListAllMethod) == null) {
      synchronized (JobMasterClientServiceGrpc.class) {
        if ((getListAllMethod = JobMasterClientServiceGrpc.getListAllMethod) == null) {
          JobMasterClientServiceGrpc.getListAllMethod = getListAllMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.ListAllPRequest, alluxio.grpc.ListAllPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListAll"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ListAllPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ListAllPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobMasterClientServiceMethodDescriptorSupplier("ListAll"))
              .build();
        }
      }
    }
    return getListAllMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RunPRequest,
      alluxio.grpc.RunPResponse> getRunMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Run",
      requestType = alluxio.grpc.RunPRequest.class,
      responseType = alluxio.grpc.RunPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.RunPRequest,
      alluxio.grpc.RunPResponse> getRunMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.RunPRequest, alluxio.grpc.RunPResponse> getRunMethod;
    if ((getRunMethod = JobMasterClientServiceGrpc.getRunMethod) == null) {
      synchronized (JobMasterClientServiceGrpc.class) {
        if ((getRunMethod = JobMasterClientServiceGrpc.getRunMethod) == null) {
          JobMasterClientServiceGrpc.getRunMethod = getRunMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.RunPRequest, alluxio.grpc.RunPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Run"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RunPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RunPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobMasterClientServiceMethodDescriptorSupplier("Run"))
              .build();
        }
      }
    }
    return getRunMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetAllWorkerHealthPRequest,
      alluxio.grpc.GetAllWorkerHealthPResponse> getGetAllWorkerHealthMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAllWorkerHealth",
      requestType = alluxio.grpc.GetAllWorkerHealthPRequest.class,
      responseType = alluxio.grpc.GetAllWorkerHealthPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetAllWorkerHealthPRequest,
      alluxio.grpc.GetAllWorkerHealthPResponse> getGetAllWorkerHealthMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetAllWorkerHealthPRequest, alluxio.grpc.GetAllWorkerHealthPResponse> getGetAllWorkerHealthMethod;
    if ((getGetAllWorkerHealthMethod = JobMasterClientServiceGrpc.getGetAllWorkerHealthMethod) == null) {
      synchronized (JobMasterClientServiceGrpc.class) {
        if ((getGetAllWorkerHealthMethod = JobMasterClientServiceGrpc.getGetAllWorkerHealthMethod) == null) {
          JobMasterClientServiceGrpc.getGetAllWorkerHealthMethod = getGetAllWorkerHealthMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetAllWorkerHealthPRequest, alluxio.grpc.GetAllWorkerHealthPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetAllWorkerHealth"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetAllWorkerHealthPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetAllWorkerHealthPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobMasterClientServiceMethodDescriptorSupplier("GetAllWorkerHealth"))
              .build();
        }
      }
    }
    return getGetAllWorkerHealthMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static JobMasterClientServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JobMasterClientServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JobMasterClientServiceStub>() {
        @java.lang.Override
        public JobMasterClientServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JobMasterClientServiceStub(channel, callOptions);
        }
      };
    return JobMasterClientServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static JobMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JobMasterClientServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JobMasterClientServiceBlockingStub>() {
        @java.lang.Override
        public JobMasterClientServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JobMasterClientServiceBlockingStub(channel, callOptions);
        }
      };
    return JobMasterClientServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static JobMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JobMasterClientServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JobMasterClientServiceFutureStub>() {
        @java.lang.Override
        public JobMasterClientServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JobMasterClientServiceFutureStub(channel, callOptions);
        }
      };
    return JobMasterClientServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   **
   * This interface contains job master service endpoints for job service clients.
   * </pre>
   */
  public static abstract class JobMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Cancels the given job.
     * </pre>
     */
    public void cancel(alluxio.grpc.CancelPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CancelPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCancelMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Gets the status of the given job.
     * </pre>
     */
    public void getJobStatus(alluxio.grpc.GetJobStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetJobStatusPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetJobStatusMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Gets the summary of the job service.
     * </pre>
     */
    public void getJobServiceSummary(alluxio.grpc.GetJobServiceSummaryPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetJobServiceSummaryPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetJobServiceSummaryMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Lists ids of all known jobs.
     * </pre>
     */
    public void listAll(alluxio.grpc.ListAllPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ListAllPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getListAllMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Starts the given job, returning a job id.
     * </pre>
     */
    public void run(alluxio.grpc.RunPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RunPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRunMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Lists all worker health.
     * </pre>
     */
    public void getAllWorkerHealth(alluxio.grpc.GetAllWorkerHealthPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetAllWorkerHealthPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetAllWorkerHealthMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCancelMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CancelPRequest,
                alluxio.grpc.CancelPResponse>(
                  this, METHODID_CANCEL)))
          .addMethod(
            getGetJobStatusMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetJobStatusPRequest,
                alluxio.grpc.GetJobStatusPResponse>(
                  this, METHODID_GET_JOB_STATUS)))
          .addMethod(
            getGetJobServiceSummaryMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetJobServiceSummaryPRequest,
                alluxio.grpc.GetJobServiceSummaryPResponse>(
                  this, METHODID_GET_JOB_SERVICE_SUMMARY)))
          .addMethod(
            getListAllMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.ListAllPRequest,
                alluxio.grpc.ListAllPResponse>(
                  this, METHODID_LIST_ALL)))
          .addMethod(
            getRunMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RunPRequest,
                alluxio.grpc.RunPResponse>(
                  this, METHODID_RUN)))
          .addMethod(
            getGetAllWorkerHealthMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetAllWorkerHealthPRequest,
                alluxio.grpc.GetAllWorkerHealthPResponse>(
                  this, METHODID_GET_ALL_WORKER_HEALTH)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains job master service endpoints for job service clients.
   * </pre>
   */
  public static final class JobMasterClientServiceStub extends io.grpc.stub.AbstractAsyncStub<JobMasterClientServiceStub> {
    private JobMasterClientServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobMasterClientServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JobMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Cancels the given job.
     * </pre>
     */
    public void cancel(alluxio.grpc.CancelPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CancelPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCancelMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Gets the status of the given job.
     * </pre>
     */
    public void getJobStatus(alluxio.grpc.GetJobStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetJobStatusPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetJobStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Gets the summary of the job service.
     * </pre>
     */
    public void getJobServiceSummary(alluxio.grpc.GetJobServiceSummaryPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetJobServiceSummaryPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetJobServiceSummaryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Lists ids of all known jobs.
     * </pre>
     */
    public void listAll(alluxio.grpc.ListAllPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ListAllPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getListAllMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Starts the given job, returning a job id.
     * </pre>
     */
    public void run(alluxio.grpc.RunPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RunPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRunMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Lists all worker health.
     * </pre>
     */
    public void getAllWorkerHealth(alluxio.grpc.GetAllWorkerHealthPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetAllWorkerHealthPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetAllWorkerHealthMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains job master service endpoints for job service clients.
   * </pre>
   */
  public static final class JobMasterClientServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<JobMasterClientServiceBlockingStub> {
    private JobMasterClientServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobMasterClientServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JobMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Cancels the given job.
     * </pre>
     */
    public alluxio.grpc.CancelPResponse cancel(alluxio.grpc.CancelPRequest request) {
      return blockingUnaryCall(
          getChannel(), getCancelMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Gets the status of the given job.
     * </pre>
     */
    public alluxio.grpc.GetJobStatusPResponse getJobStatus(alluxio.grpc.GetJobStatusPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetJobStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Gets the summary of the job service.
     * </pre>
     */
    public alluxio.grpc.GetJobServiceSummaryPResponse getJobServiceSummary(alluxio.grpc.GetJobServiceSummaryPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetJobServiceSummaryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Lists ids of all known jobs.
     * </pre>
     */
    public alluxio.grpc.ListAllPResponse listAll(alluxio.grpc.ListAllPRequest request) {
      return blockingUnaryCall(
          getChannel(), getListAllMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Starts the given job, returning a job id.
     * </pre>
     */
    public alluxio.grpc.RunPResponse run(alluxio.grpc.RunPRequest request) {
      return blockingUnaryCall(
          getChannel(), getRunMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Lists all worker health.
     * </pre>
     */
    public alluxio.grpc.GetAllWorkerHealthPResponse getAllWorkerHealth(alluxio.grpc.GetAllWorkerHealthPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetAllWorkerHealthMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains job master service endpoints for job service clients.
   * </pre>
   */
  public static final class JobMasterClientServiceFutureStub extends io.grpc.stub.AbstractFutureStub<JobMasterClientServiceFutureStub> {
    private JobMasterClientServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobMasterClientServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JobMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Cancels the given job.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CancelPResponse> cancel(
        alluxio.grpc.CancelPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCancelMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Gets the status of the given job.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetJobStatusPResponse> getJobStatus(
        alluxio.grpc.GetJobStatusPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetJobStatusMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Gets the summary of the job service.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetJobServiceSummaryPResponse> getJobServiceSummary(
        alluxio.grpc.GetJobServiceSummaryPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetJobServiceSummaryMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Lists ids of all known jobs.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.ListAllPResponse> listAll(
        alluxio.grpc.ListAllPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getListAllMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Starts the given job, returning a job id.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RunPResponse> run(
        alluxio.grpc.RunPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRunMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Lists all worker health.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetAllWorkerHealthPResponse> getAllWorkerHealth(
        alluxio.grpc.GetAllWorkerHealthPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetAllWorkerHealthMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CANCEL = 0;
  private static final int METHODID_GET_JOB_STATUS = 1;
  private static final int METHODID_GET_JOB_SERVICE_SUMMARY = 2;
  private static final int METHODID_LIST_ALL = 3;
  private static final int METHODID_RUN = 4;
  private static final int METHODID_GET_ALL_WORKER_HEALTH = 5;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final JobMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(JobMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CANCEL:
          serviceImpl.cancel((alluxio.grpc.CancelPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CancelPResponse>) responseObserver);
          break;
        case METHODID_GET_JOB_STATUS:
          serviceImpl.getJobStatus((alluxio.grpc.GetJobStatusPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetJobStatusPResponse>) responseObserver);
          break;
        case METHODID_GET_JOB_SERVICE_SUMMARY:
          serviceImpl.getJobServiceSummary((alluxio.grpc.GetJobServiceSummaryPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetJobServiceSummaryPResponse>) responseObserver);
          break;
        case METHODID_LIST_ALL:
          serviceImpl.listAll((alluxio.grpc.ListAllPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.ListAllPResponse>) responseObserver);
          break;
        case METHODID_RUN:
          serviceImpl.run((alluxio.grpc.RunPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RunPResponse>) responseObserver);
          break;
        case METHODID_GET_ALL_WORKER_HEALTH:
          serviceImpl.getAllWorkerHealth((alluxio.grpc.GetAllWorkerHealthPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetAllWorkerHealthPResponse>) responseObserver);
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

  private static abstract class JobMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    JobMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.JobMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("JobMasterClientService");
    }
  }

  private static final class JobMasterClientServiceFileDescriptorSupplier
      extends JobMasterClientServiceBaseDescriptorSupplier {
    JobMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class JobMasterClientServiceMethodDescriptorSupplier
      extends JobMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    JobMasterClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (JobMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new JobMasterClientServiceFileDescriptorSupplier())
              .addMethod(getCancelMethod())
              .addMethod(getGetJobStatusMethod())
              .addMethod(getGetJobServiceSummaryMethod())
              .addMethod(getListAllMethod())
              .addMethod(getRunMethod())
              .addMethod(getGetAllWorkerHealthMethod())
              .build();
        }
      }
    }
    return result;
  }
}
