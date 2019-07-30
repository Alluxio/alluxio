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
 * This interface contains catalog master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: grpc/catalog_master.proto")
public final class CatalogMasterClientServiceGrpc {

  private CatalogMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.CatalogMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetAllDatabasesPRequest,
      alluxio.grpc.GetAllDatabasesPResponse> getGetAllDatabasesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAllDatabases",
      requestType = alluxio.grpc.GetAllDatabasesPRequest.class,
      responseType = alluxio.grpc.GetAllDatabasesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetAllDatabasesPRequest,
      alluxio.grpc.GetAllDatabasesPResponse> getGetAllDatabasesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetAllDatabasesPRequest, alluxio.grpc.GetAllDatabasesPResponse> getGetAllDatabasesMethod;
    if ((getGetAllDatabasesMethod = CatalogMasterClientServiceGrpc.getGetAllDatabasesMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetAllDatabasesMethod = CatalogMasterClientServiceGrpc.getGetAllDatabasesMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetAllDatabasesMethod = getGetAllDatabasesMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetAllDatabasesPRequest, alluxio.grpc.GetAllDatabasesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.CatalogMasterClientService", "GetAllDatabases"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetAllDatabasesPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetAllDatabasesPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetAllDatabases"))
                  .build();
          }
        }
     }
     return getGetAllDatabasesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CatalogMasterClientServiceStub newStub(io.grpc.Channel channel) {
    return new CatalogMasterClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CatalogMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new CatalogMasterClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CatalogMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new CatalogMasterClientServiceFutureStub(channel);
  }

  /**
   * <pre>
   **
   * This interface contains catalog master service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class CatalogMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public void getAllDatabases(alluxio.grpc.GetAllDatabasesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetAllDatabasesPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetAllDatabasesMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetAllDatabasesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetAllDatabasesPRequest,
                alluxio.grpc.GetAllDatabasesPResponse>(
                  this, METHODID_GET_ALL_DATABASES)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains catalog master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class CatalogMasterClientServiceStub extends io.grpc.stub.AbstractStub<CatalogMasterClientServiceStub> {
    private CatalogMasterClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CatalogMasterClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CatalogMasterClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CatalogMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public void getAllDatabases(alluxio.grpc.GetAllDatabasesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetAllDatabasesPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetAllDatabasesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains catalog master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class CatalogMasterClientServiceBlockingStub extends io.grpc.stub.AbstractStub<CatalogMasterClientServiceBlockingStub> {
    private CatalogMasterClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CatalogMasterClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CatalogMasterClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CatalogMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public alluxio.grpc.GetAllDatabasesPResponse getAllDatabases(alluxio.grpc.GetAllDatabasesPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetAllDatabasesMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains catalog master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class CatalogMasterClientServiceFutureStub extends io.grpc.stub.AbstractStub<CatalogMasterClientServiceFutureStub> {
    private CatalogMasterClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CatalogMasterClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CatalogMasterClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CatalogMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetAllDatabasesPResponse> getAllDatabases(
        alluxio.grpc.GetAllDatabasesPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetAllDatabasesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_ALL_DATABASES = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CatalogMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(CatalogMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_ALL_DATABASES:
          serviceImpl.getAllDatabases((alluxio.grpc.GetAllDatabasesPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetAllDatabasesPResponse>) responseObserver);
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

  private static abstract class CatalogMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CatalogMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.CatalogMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CatalogMasterClientService");
    }
  }

  private static final class CatalogMasterClientServiceFileDescriptorSupplier
      extends CatalogMasterClientServiceBaseDescriptorSupplier {
    CatalogMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class CatalogMasterClientServiceMethodDescriptorSupplier
      extends CatalogMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    CatalogMasterClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (CatalogMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CatalogMasterClientServiceFileDescriptorSupplier())
              .addMethod(getGetAllDatabasesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
