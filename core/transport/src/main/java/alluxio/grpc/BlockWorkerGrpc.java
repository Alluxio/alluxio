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
    comments = "Source: block_worker.proto")
public final class BlockWorkerGrpc {

  private BlockWorkerGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.BlockWorker";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getReadBlockMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.ReadRequest,
      alluxio.grpc.ReadResponse> METHOD_READ_BLOCK = getReadBlockMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ReadRequest,
      alluxio.grpc.ReadResponse> getReadBlockMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.ReadRequest,
      alluxio.grpc.ReadResponse> getReadBlockMethod() {
    return getReadBlockMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.ReadRequest,
      alluxio.grpc.ReadResponse> getReadBlockMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.ReadRequest, alluxio.grpc.ReadResponse> getReadBlockMethod;
    if ((getReadBlockMethod = BlockWorkerGrpc.getReadBlockMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getReadBlockMethod = BlockWorkerGrpc.getReadBlockMethod) == null) {
          BlockWorkerGrpc.getReadBlockMethod = getReadBlockMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.ReadRequest, alluxio.grpc.ReadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorker", "ReadBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ReadResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("ReadBlock"))
                  .build();
          }
        }
     }
     return getReadBlockMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getWriteBlockMethod()} instead. 
  public static final io.grpc.MethodDescriptor<alluxio.grpc.WriteRequest,
      alluxio.grpc.WriteResponse> METHOD_WRITE_BLOCK = getWriteBlockMethodHelper();

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.WriteRequest,
      alluxio.grpc.WriteResponse> getWriteBlockMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<alluxio.grpc.WriteRequest,
      alluxio.grpc.WriteResponse> getWriteBlockMethod() {
    return getWriteBlockMethodHelper();
  }

  private static io.grpc.MethodDescriptor<alluxio.grpc.WriteRequest,
      alluxio.grpc.WriteResponse> getWriteBlockMethodHelper() {
    io.grpc.MethodDescriptor<alluxio.grpc.WriteRequest, alluxio.grpc.WriteResponse> getWriteBlockMethod;
    if ((getWriteBlockMethod = BlockWorkerGrpc.getWriteBlockMethod) == null) {
      synchronized (BlockWorkerGrpc.class) {
        if ((getWriteBlockMethod = BlockWorkerGrpc.getWriteBlockMethod) == null) {
          BlockWorkerGrpc.getWriteBlockMethod = getWriteBlockMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.WriteRequest, alluxio.grpc.WriteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.BlockWorker", "WriteBlock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.WriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.WriteResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new BlockWorkerMethodDescriptorSupplier("WriteBlock"))
                  .build();
          }
        }
     }
     return getWriteBlockMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BlockWorkerStub newStub(io.grpc.Channel channel) {
    return new BlockWorkerStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BlockWorkerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BlockWorkerBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BlockWorkerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BlockWorkerFutureStub(channel);
  }

  /**
   */
  public static abstract class BlockWorkerImplBase implements io.grpc.BindableService {

    /**
     */
    public void readBlock(alluxio.grpc.ReadRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ReadResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getReadBlockMethodHelper(), responseObserver);
    }

    /**
     * <pre>
     * TODO(feng): Add more worker RPC
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.WriteRequest> writeBlock(
        io.grpc.stub.StreamObserver<alluxio.grpc.WriteResponse> responseObserver) {
      return asyncUnimplementedStreamingCall(getWriteBlockMethodHelper(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getReadBlockMethodHelper(),
            asyncServerStreamingCall(
              new MethodHandlers<
                alluxio.grpc.ReadRequest,
                alluxio.grpc.ReadResponse>(
                  this, METHODID_READ_BLOCK)))
          .addMethod(
            getWriteBlockMethodHelper(),
            asyncBidiStreamingCall(
              new MethodHandlers<
                alluxio.grpc.WriteRequest,
                alluxio.grpc.WriteResponse>(
                  this, METHODID_WRITE_BLOCK)))
          .build();
    }
  }

  /**
   */
  public static final class BlockWorkerStub extends io.grpc.stub.AbstractStub<BlockWorkerStub> {
    private BlockWorkerStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockWorkerStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockWorkerStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockWorkerStub(channel, callOptions);
    }

    /**
     */
    public void readBlock(alluxio.grpc.ReadRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ReadResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getReadBlockMethodHelper(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * TODO(feng): Add more worker RPC
     * </pre>
     */
    public io.grpc.stub.StreamObserver<alluxio.grpc.WriteRequest> writeBlock(
        io.grpc.stub.StreamObserver<alluxio.grpc.WriteResponse> responseObserver) {
      return asyncBidiStreamingCall(
          getChannel().newCall(getWriteBlockMethodHelper(), getCallOptions()), responseObserver);
    }
  }

  /**
   */
  public static final class BlockWorkerBlockingStub extends io.grpc.stub.AbstractStub<BlockWorkerBlockingStub> {
    private BlockWorkerBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockWorkerBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockWorkerBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockWorkerBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<alluxio.grpc.ReadResponse> readBlock(
        alluxio.grpc.ReadRequest request) {
      return blockingServerStreamingCall(
          getChannel(), getReadBlockMethodHelper(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class BlockWorkerFutureStub extends io.grpc.stub.AbstractStub<BlockWorkerFutureStub> {
    private BlockWorkerFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BlockWorkerFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BlockWorkerFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BlockWorkerFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_READ_BLOCK = 0;
  private static final int METHODID_WRITE_BLOCK = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BlockWorkerImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BlockWorkerImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_READ_BLOCK:
          serviceImpl.readBlock((alluxio.grpc.ReadRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.ReadResponse>) responseObserver);
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
        case METHODID_WRITE_BLOCK:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.writeBlock(
              (io.grpc.stub.StreamObserver<alluxio.grpc.WriteResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class BlockWorkerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BlockWorkerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.BlockWorkerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BlockWorker");
    }
  }

  private static final class BlockWorkerFileDescriptorSupplier
      extends BlockWorkerBaseDescriptorSupplier {
    BlockWorkerFileDescriptorSupplier() {}
  }

  private static final class BlockWorkerMethodDescriptorSupplier
      extends BlockWorkerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BlockWorkerMethodDescriptorSupplier(String methodName) {
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
      synchronized (BlockWorkerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BlockWorkerFileDescriptorSupplier())
              .addMethod(getReadBlockMethodHelper())
              .addMethod(getWriteBlockMethodHelper())
              .build();
        }
      }
    }
    return result;
  }
}
