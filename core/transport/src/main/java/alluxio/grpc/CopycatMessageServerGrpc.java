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
 * The copycat message server service.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: grpc/copycat.proto")
public final class CopycatMessageServerGrpc {

  private CopycatMessageServerGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.copycat.CopycatMessageServer";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CopycatMessage,
      alluxio.grpc.CopycatMessage> getConnectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "connect",
      requestType = alluxio.grpc.CopycatMessage.class,
      responseType = alluxio.grpc.CopycatMessage.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CopycatMessage,
      alluxio.grpc.CopycatMessage> getConnectMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CopycatMessage, alluxio.grpc.CopycatMessage> getConnectMethod;
    if ((getConnectMethod = CopycatMessageServerGrpc.getConnectMethod) == null) {
      synchronized (CopycatMessageServerGrpc.class) {
        if ((getConnectMethod = CopycatMessageServerGrpc.getConnectMethod) == null) {
          CopycatMessageServerGrpc.getConnectMethod = getConnectMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CopycatMessage, alluxio.grpc.CopycatMessage>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.copycat.CopycatMessageServer", "connect"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CopycatMessage.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CopycatMessage.getDefaultInstance()))
                  .setSchemaDescriptor(new CopycatMessageServerMethodDescriptorSupplier("connect"))
                  .build();
          }
        }
     }
     return getConnectMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CopycatMessageServerStub newStub(io.grpc.Channel channel) {
    return new CopycatMessageServerStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CopycatMessageServerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new CopycatMessageServerBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CopycatMessageServerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new CopycatMessageServerFutureStub(channel);
  }

  /**
   * <pre>
   * The copycat message server service.
   * </pre>
   */
  public static abstract class CopycatMessageServerImplBase implements io.grpc.BindableService {

    /**
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.CopycatMessage> connect(
        io.grpc.stub.StreamObserver<alluxio.grpc.CopycatMessage> responseObserver) {
      return asyncUnimplementedStreamingCall(getConnectMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getConnectMethod(),
            asyncBidiStreamingCall(
              new MethodHandlers<
                alluxio.grpc.CopycatMessage,
                alluxio.grpc.CopycatMessage>(
                  this, METHODID_CONNECT)))
          .build();
    }
  }

  /**
   * <pre>
   * The copycat message server service.
   * </pre>
   */
  public static final class CopycatMessageServerStub extends io.grpc.stub.AbstractStub<CopycatMessageServerStub> {
    private CopycatMessageServerStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CopycatMessageServerStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CopycatMessageServerStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CopycatMessageServerStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.CopycatMessage> connect(
        io.grpc.stub.StreamObserver<alluxio.grpc.CopycatMessage> responseObserver) {
      return asyncBidiStreamingCall(
          getChannel().newCall(getConnectMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * <pre>
   * The copycat message server service.
   * </pre>
   */
  public static final class CopycatMessageServerBlockingStub extends io.grpc.stub.AbstractStub<CopycatMessageServerBlockingStub> {
    private CopycatMessageServerBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CopycatMessageServerBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CopycatMessageServerBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CopycatMessageServerBlockingStub(channel, callOptions);
    }
  }

  /**
   * <pre>
   * The copycat message server service.
   * </pre>
   */
  public static final class CopycatMessageServerFutureStub extends io.grpc.stub.AbstractStub<CopycatMessageServerFutureStub> {
    private CopycatMessageServerFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CopycatMessageServerFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CopycatMessageServerFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CopycatMessageServerFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_CONNECT = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CopycatMessageServerImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(CopycatMessageServerImplBase serviceImpl, int methodId) {
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
        case METHODID_CONNECT:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.connect(
              (io.grpc.stub.StreamObserver<alluxio.grpc.CopycatMessage>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class CopycatMessageServerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CopycatMessageServerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.CopycatProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CopycatMessageServer");
    }
  }

  private static final class CopycatMessageServerFileDescriptorSupplier
      extends CopycatMessageServerBaseDescriptorSupplier {
    CopycatMessageServerFileDescriptorSupplier() {}
  }

  private static final class CopycatMessageServerMethodDescriptorSupplier
      extends CopycatMessageServerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    CopycatMessageServerMethodDescriptorSupplier(String methodName) {
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
      synchronized (CopycatMessageServerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CopycatMessageServerFileDescriptorSupplier())
              .addMethod(getConnectMethod())
              .build();
        }
      }
    }
    return result;
  }
}
