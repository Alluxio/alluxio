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
    value = "by gRPC proto compiler (version 1.10.1)",
    comments = "Source: job_master.proto")
public final class JobMasterClientServiceGrpc {

  private JobMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.JobMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getCancelMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.CancelPRequest,
      alluxio.grpc.CancelPResponse> METHOD_CANCEL = getCancelMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CancelPRequest,
      alluxio.grpc.CancelPResponse> getCancelMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.CancelPRequest,
      alluxio.grpc.CancelPResponse> getCancelMethod() {
    return getCancelMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.CancelPRequest,
      alluxio.grpc.CancelPResponse> getCancelMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.CancelPRequest, alluxio.grpc.CancelPResponse> getCancelMethod;
    if ((getCancelMethod = JobMasterClientServiceGrpc.getCancelMethod) == null) {
      synchronized (JobMasterClientServiceGrpc.class) {
        if ((getCancelMethod = JobMasterClientServiceGrpc.getCancelMethod) == null) {
          JobMasterClientServiceGrpc.getCancelMethod = getCancelMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CancelPRequest, alluxio.grpc.CancelPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.JobMasterClientService", "Cancel"))
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
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getGetJobStatusMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.GetJobStatusPRequest,
      alluxio.grpc.GetJobStatusPResponse> METHOD_GET_JOB_STATUS = getGetJobStatusMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetJobStatusPRequest,
      alluxio.grpc.GetJobStatusPResponse> getGetJobStatusMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetJobStatusPRequest,
      alluxio.grpc.GetJobStatusPResponse> getGetJobStatusMethod() {
    return getGetJobStatusMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.GetJobStatusPRequest,
      alluxio.grpc.GetJobStatusPResponse> getGetJobStatusMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetJobStatusPRequest, alluxio.grpc.GetJobStatusPResponse> getGetJobStatusMethod;
    if ((getGetJobStatusMethod = JobMasterClientServiceGrpc.getGetJobStatusMethod) == null) {
      synchronized (JobMasterClientServiceGrpc.class) {
        if ((getGetJobStatusMethod = JobMasterClientServiceGrpc.getGetJobStatusMethod) == null) {
          JobMasterClientServiceGrpc.getGetJobStatusMethod = getGetJobStatusMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetJobStatusPRequest, alluxio.grpc.GetJobStatusPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.JobMasterClientService", "GetJobStatus"))
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
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getListAllMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.ListAllPRequest,
      alluxio.grpc.ListAllPResponse> METHOD_LIST_ALL = getListAllMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ListAllPRequest,
      alluxio.grpc.ListAllPResponse> getListAllMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.ListAllPRequest,
      alluxio.grpc.ListAllPResponse> getListAllMethod() {
    return getListAllMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.ListAllPRequest,
      alluxio.grpc.ListAllPResponse> getListAllMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.ListAllPRequest, alluxio.grpc.ListAllPResponse> getListAllMethod;
    if ((getListAllMethod = JobMasterClientServiceGrpc.getListAllMethod) == null) {
      synchronized (JobMasterClientServiceGrpc.class) {
        if ((getListAllMethod = JobMasterClientServiceGrpc.getListAllMethod) == null) {
          JobMasterClientServiceGrpc.getListAllMethod = getListAllMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.ListAllPRequest, alluxio.grpc.ListAllPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.JobMasterClientService", "ListAll"))
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
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getRunMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.RunPRequest,
      alluxio.grpc.RunPResponse> METHOD_RUN = getRunMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RunPRequest,
      alluxio.grpc.RunPResponse> getRunMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.RunPRequest,
      alluxio.grpc.RunPResponse> getRunMethod() {
    return getRunMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.RunPRequest,
      alluxio.grpc.RunPResponse> getRunMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.RunPRequest, alluxio.grpc.RunPResponse> getRunMethod;
    if ((getRunMethod = JobMasterClientServiceGrpc.getRunMethod) == null) {
      synchronized (JobMasterClientServiceGrpc.class) {
        if ((getRunMethod = JobMasterClientServiceGrpc.getRunMethod) == null) {
          JobMasterClientServiceGrpc.getRunMethod = getRunMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.RunPRequest, alluxio.grpc.RunPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.JobMasterClientService", "Run"))
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

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static JobMasterClientServiceStub newStub(io.grpc.Channel channel) {
    return new JobMasterClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static JobMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new JobMasterClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static JobMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new JobMasterClientServiceFutureStub(channel);
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
      asyncUnimplementedUnaryCall(getCancelMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Gets the status of the given job.
     * </pre>
     */
    public void getJobStatus(alluxio.grpc.GetJobStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetJobStatusPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetJobStatusMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Lists ids of all known jobs.
     * </pre>
     */
    public void listAll(alluxio.grpc.ListAllPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ListAllPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getListAllMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     **
     * Starts the given job, returning a job id.
     * </pre>
     */
    public void run(alluxio.grpc.RunPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RunPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRunMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCancelMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CancelPRequest,
                alluxio.grpc.CancelPResponse>(
                  this, METHODID_CANCEL)))
          .addMethod(
            getGetJobStatusMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetJobStatusPRequest,
                alluxio.grpc.GetJobStatusPResponse>(
                  this, METHODID_GET_JOB_STATUS)))
          .addMethod(
            getListAllMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.ListAllPRequest,
                alluxio.grpc.ListAllPResponse>(
                  this, METHODID_LIST_ALL)))
          .addMethod(
            getRunMethodHelper(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RunPRequest,
                alluxio.grpc.RunPResponse>(
                  this, METHODID_RUN)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains job master service endpoints for job service clients.
   * </pre>
   */
  public static final class JobMasterClientServiceStub extends io.grpc.stub.AbstractStub<JobMasterClientServiceStub> {
    private JobMasterClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private JobMasterClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobMasterClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
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
          getChannel().newCall(getCancelMethodHelper(), getCallOptions()), request, responseObserver);
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
          getChannel().newCall(getGetJobStatusMethodHelper(), getCallOptions()), request, responseObserver);
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
          getChannel().newCall(getListAllMethodHelper(), getCallOptions()), request, responseObserver);
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
          getChannel().newCall(getRunMethodHelper(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains job master service endpoints for job service clients.
   * </pre>
   */
  public static final class JobMasterClientServiceBlockingStub extends io.grpc.stub.AbstractStub<JobMasterClientServiceBlockingStub> {
    private JobMasterClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private JobMasterClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobMasterClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
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
          getChannel(), getCancelMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Gets the status of the given job.
     * </pre>
     */
    public alluxio.grpc.GetJobStatusPResponse getJobStatus(alluxio.grpc.GetJobStatusPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetJobStatusMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Lists ids of all known jobs.
     * </pre>
     */
    public alluxio.grpc.ListAllPResponse listAll(alluxio.grpc.ListAllPRequest request) {
      return blockingUnaryCall(
          getChannel(), getListAllMethodHelper(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Starts the given job, returning a job id.
     * </pre>
     */
    public alluxio.grpc.RunPResponse run(alluxio.grpc.RunPRequest request) {
      return blockingUnaryCall(
          getChannel(), getRunMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains job master service endpoints for job service clients.
   * </pre>
   */
  public static final class JobMasterClientServiceFutureStub extends io.grpc.stub.AbstractStub<JobMasterClientServiceFutureStub> {
    private JobMasterClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private JobMasterClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobMasterClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
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
          getChannel().newCall(getCancelMethodHelper(), getCallOptions()), request);
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
          getChannel().newCall(getGetJobStatusMethodHelper(), getCallOptions()), request);
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
          getChannel().newCall(getListAllMethodHelper(), getCallOptions()), request);
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
          getChannel().newCall(getRunMethodHelper(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CANCEL = 0;
  private static final int METHODID_GET_JOB_STATUS = 1;
  private static final int METHODID_LIST_ALL = 2;
  private static final int METHODID_RUN = 3;

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
        case METHODID_LIST_ALL:
          serviceImpl.listAll((alluxio.grpc.ListAllPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.ListAllPResponse>) responseObserver);
          break;
        case METHODID_RUN:
          serviceImpl.run((alluxio.grpc.RunPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RunPResponse>) responseObserver);
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
              .addMethod(getCancelMethodHelper())
              .addMethod(getGetJobStatusMethodHelper())
              .addMethod(getListAllMethodHelper())
              .addMethod(getRunMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
