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
 * This interface contains journal master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.27.0)",
    comments = "Source: grpc/journal_master.proto")
public final class JournalMasterClientServiceGrpc {

  private JournalMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.journal.JournalMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetQuorumInfoPRequest,
      alluxio.grpc.GetQuorumInfoPResponse> getGetQuorumInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetQuorumInfo",
      requestType = alluxio.grpc.GetQuorumInfoPRequest.class,
      responseType = alluxio.grpc.GetQuorumInfoPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetQuorumInfoPRequest,
      alluxio.grpc.GetQuorumInfoPResponse> getGetQuorumInfoMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetQuorumInfoPRequest, alluxio.grpc.GetQuorumInfoPResponse> getGetQuorumInfoMethod;
    if ((getGetQuorumInfoMethod = JournalMasterClientServiceGrpc.getGetQuorumInfoMethod) == null) {
      synchronized (JournalMasterClientServiceGrpc.class) {
        if ((getGetQuorumInfoMethod = JournalMasterClientServiceGrpc.getGetQuorumInfoMethod) == null) {
          JournalMasterClientServiceGrpc.getGetQuorumInfoMethod = getGetQuorumInfoMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetQuorumInfoPRequest, alluxio.grpc.GetQuorumInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetQuorumInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetQuorumInfoPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetQuorumInfoPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JournalMasterClientServiceMethodDescriptorSupplier("GetQuorumInfo"))
              .build();
        }
      }
    }
    return getGetQuorumInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RemoveQuorumServerPRequest,
      alluxio.grpc.RemoveQuorumServerPResponse> getRemoveQuorumServerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RemoveQuorumServer",
      requestType = alluxio.grpc.RemoveQuorumServerPRequest.class,
      responseType = alluxio.grpc.RemoveQuorumServerPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.RemoveQuorumServerPRequest,
      alluxio.grpc.RemoveQuorumServerPResponse> getRemoveQuorumServerMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.RemoveQuorumServerPRequest, alluxio.grpc.RemoveQuorumServerPResponse> getRemoveQuorumServerMethod;
    if ((getRemoveQuorumServerMethod = JournalMasterClientServiceGrpc.getRemoveQuorumServerMethod) == null) {
      synchronized (JournalMasterClientServiceGrpc.class) {
        if ((getRemoveQuorumServerMethod = JournalMasterClientServiceGrpc.getRemoveQuorumServerMethod) == null) {
          JournalMasterClientServiceGrpc.getRemoveQuorumServerMethod = getRemoveQuorumServerMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.RemoveQuorumServerPRequest, alluxio.grpc.RemoveQuorumServerPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RemoveQuorumServer"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RemoveQuorumServerPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RemoveQuorumServerPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JournalMasterClientServiceMethodDescriptorSupplier("RemoveQuorumServer"))
              .build();
        }
      }
    }
    return getRemoveQuorumServerMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static JournalMasterClientServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JournalMasterClientServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JournalMasterClientServiceStub>() {
        @java.lang.Override
        public JournalMasterClientServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JournalMasterClientServiceStub(channel, callOptions);
        }
      };
    return JournalMasterClientServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static JournalMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JournalMasterClientServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JournalMasterClientServiceBlockingStub>() {
        @java.lang.Override
        public JournalMasterClientServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JournalMasterClientServiceBlockingStub(channel, callOptions);
        }
      };
    return JournalMasterClientServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static JournalMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JournalMasterClientServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JournalMasterClientServiceFutureStub>() {
        @java.lang.Override
        public JournalMasterClientServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JournalMasterClientServiceFutureStub(channel, callOptions);
        }
      };
    return JournalMasterClientServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   **
   * This interface contains journal master service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class JournalMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Gets list of server states in an embedded journal quorum.
     * </pre>
     */
    public void getQuorumInfo(alluxio.grpc.GetQuorumInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetQuorumInfoPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetQuorumInfoMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Removes a server from embedded journal quorum.
     * </pre>
     */
    public void removeQuorumServer(alluxio.grpc.RemoveQuorumServerPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RemoveQuorumServerPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRemoveQuorumServerMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetQuorumInfoMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetQuorumInfoPRequest,
                alluxio.grpc.GetQuorumInfoPResponse>(
                  this, METHODID_GET_QUORUM_INFO)))
          .addMethod(
            getRemoveQuorumServerMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RemoveQuorumServerPRequest,
                alluxio.grpc.RemoveQuorumServerPResponse>(
                  this, METHODID_REMOVE_QUORUM_SERVER)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains journal master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class JournalMasterClientServiceStub extends io.grpc.stub.AbstractAsyncStub<JournalMasterClientServiceStub> {
    private JournalMasterClientServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JournalMasterClientServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JournalMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Gets list of server states in an embedded journal quorum.
     * </pre>
     */
    public void getQuorumInfo(alluxio.grpc.GetQuorumInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetQuorumInfoPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetQuorumInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Removes a server from embedded journal quorum.
     * </pre>
     */
    public void removeQuorumServer(alluxio.grpc.RemoveQuorumServerPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RemoveQuorumServerPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRemoveQuorumServerMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains journal master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class JournalMasterClientServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<JournalMasterClientServiceBlockingStub> {
    private JournalMasterClientServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JournalMasterClientServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JournalMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Gets list of server states in an embedded journal quorum.
     * </pre>
     */
    public alluxio.grpc.GetQuorumInfoPResponse getQuorumInfo(alluxio.grpc.GetQuorumInfoPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetQuorumInfoMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Removes a server from embedded journal quorum.
     * </pre>
     */
    public alluxio.grpc.RemoveQuorumServerPResponse removeQuorumServer(alluxio.grpc.RemoveQuorumServerPRequest request) {
      return blockingUnaryCall(
          getChannel(), getRemoveQuorumServerMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains journal master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class JournalMasterClientServiceFutureStub extends io.grpc.stub.AbstractFutureStub<JournalMasterClientServiceFutureStub> {
    private JournalMasterClientServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JournalMasterClientServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JournalMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Gets list of server states in an embedded journal quorum.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetQuorumInfoPResponse> getQuorumInfo(
        alluxio.grpc.GetQuorumInfoPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetQuorumInfoMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Removes a server from embedded journal quorum.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RemoveQuorumServerPResponse> removeQuorumServer(
        alluxio.grpc.RemoveQuorumServerPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRemoveQuorumServerMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_QUORUM_INFO = 0;
  private static final int METHODID_REMOVE_QUORUM_SERVER = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final JournalMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(JournalMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_QUORUM_INFO:
          serviceImpl.getQuorumInfo((alluxio.grpc.GetQuorumInfoPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetQuorumInfoPResponse>) responseObserver);
          break;
        case METHODID_REMOVE_QUORUM_SERVER:
          serviceImpl.removeQuorumServer((alluxio.grpc.RemoveQuorumServerPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RemoveQuorumServerPResponse>) responseObserver);
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

  private static abstract class JournalMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    JournalMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.JournalMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("JournalMasterClientService");
    }
  }

  private static final class JournalMasterClientServiceFileDescriptorSupplier
      extends JournalMasterClientServiceBaseDescriptorSupplier {
    JournalMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class JournalMasterClientServiceMethodDescriptorSupplier
      extends JournalMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    JournalMasterClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (JournalMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new JournalMasterClientServiceFileDescriptorSupplier())
              .addMethod(getGetQuorumInfoMethod())
              .addMethod(getRemoveQuorumServerMethod())
              .build();
        }
      }
    }
    return result;
  }
}
