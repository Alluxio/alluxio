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
    comments = "Source: sasl_server.proto")
public final class AlluxioSaslClientServiceGrpc {

  private AlluxioSaslClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.AlluxioSaslClientService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getAuthenticateMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.SaslMessage,
      alluxio.grpc.SaslMessage> METHOD_AUTHENTICATE = getAuthenticateMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.SaslMessage,
      alluxio.grpc.SaslMessage> getAuthenticateMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.SaslMessage,
      alluxio.grpc.SaslMessage> getAuthenticateMethod() {
    return getAuthenticateMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.SaslMessage,
      alluxio.grpc.SaslMessage> getAuthenticateMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.SaslMessage, alluxio.grpc.SaslMessage> getAuthenticateMethod;
    if ((getAuthenticateMethod = AlluxioSaslClientServiceGrpc.getAuthenticateMethod) == null) {
      synchronized (AlluxioSaslClientServiceGrpc.class) {
        if ((getAuthenticateMethod = AlluxioSaslClientServiceGrpc.getAuthenticateMethod) == null) {
          AlluxioSaslClientServiceGrpc.getAuthenticateMethod = getAuthenticateMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.SaslMessage, alluxio.grpc.SaslMessage>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.AlluxioSaslClientService", "authenticate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SaslMessage.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SaslMessage.getDefaultInstance()))
                  .setSchemaDescriptor(new AlluxioSaslClientServiceMethodDescriptorSupplier("authenticate"))
                  .build();
          }
        }
     }
     return getAuthenticateMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AlluxioSaslClientServiceStub newStub(io.grpc.Channel channel) {
    return new AlluxioSaslClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AlluxioSaslClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new AlluxioSaslClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AlluxioSaslClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new AlluxioSaslClientServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class AlluxioSaslClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Used to drive Sasl negotiation with clients.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.SaslMessage> authenticate(
        io.grpc.stub.StreamObserver<alluxio.grpc.SaslMessage> responseObserver) {
      return asyncUnimplementedStreamingCall(getAuthenticateMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getAuthenticateMethodHelper(),
            asyncBidiStreamingCall(
              new MethodHandlers<
                alluxio.grpc.SaslMessage,
                alluxio.grpc.SaslMessage>(
                  this, METHODID_AUTHENTICATE)))
          .build();
    }
  }

  /**
   */
  public static final class AlluxioSaslClientServiceStub extends io.grpc.stub.AbstractStub<AlluxioSaslClientServiceStub> {
    private AlluxioSaslClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AlluxioSaslClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AlluxioSaslClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AlluxioSaslClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Used to drive Sasl negotiation with clients.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.SaslMessage> authenticate(
        io.grpc.stub.StreamObserver<alluxio.grpc.SaslMessage> responseObserver) {
      return asyncBidiStreamingCall(
          getChannel().newCall(getAuthenticateMethodHelper(), getCallOptions()), responseObserver);
    }
  }

  /**
   */
  public static final class AlluxioSaslClientServiceBlockingStub extends io.grpc.stub.AbstractStub<AlluxioSaslClientServiceBlockingStub> {
    private AlluxioSaslClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AlluxioSaslClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AlluxioSaslClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AlluxioSaslClientServiceBlockingStub(channel, callOptions);
    }
  }

  /**
   */
  public static final class AlluxioSaslClientServiceFutureStub extends io.grpc.stub.AbstractStub<AlluxioSaslClientServiceFutureStub> {
    private AlluxioSaslClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AlluxioSaslClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AlluxioSaslClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AlluxioSaslClientServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_AUTHENTICATE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AlluxioSaslClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(AlluxioSaslClientServiceImplBase serviceImpl, int methodId) {
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
        case METHODID_AUTHENTICATE:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.authenticate(
              (io.grpc.stub.StreamObserver<alluxio.grpc.SaslMessage>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class AlluxioSaslClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AlluxioSaslClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.AuthenticationServerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("AlluxioSaslClientService");
    }
  }

  private static final class AlluxioSaslClientServiceFileDescriptorSupplier
      extends AlluxioSaslClientServiceBaseDescriptorSupplier {
    AlluxioSaslClientServiceFileDescriptorSupplier() {}
  }

  private static final class AlluxioSaslClientServiceMethodDescriptorSupplier
      extends AlluxioSaslClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    AlluxioSaslClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (AlluxioSaslClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AlluxioSaslClientServiceFileDescriptorSupplier())
              .addMethod(getAuthenticateMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
