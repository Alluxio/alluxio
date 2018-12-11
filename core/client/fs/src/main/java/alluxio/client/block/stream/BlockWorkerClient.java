package alluxio.client.block.stream;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.CreateUfsBlockOptions;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.util.grpc.GrpcChannel;
import alluxio.util.grpc.GrpcChannelBuilder;
import alluxio.util.network.NettyUtils;

import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.stub.StreamObserver;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.security.auth.Subject;

/**
 * gRPC client for worker communication.
 */
public class BlockWorkerClient implements Closeable {
  private static final Logger LOGGER =
      Logger.getLogger(BlockWorkerClient.class.getName());

  GrpcChannel mChannel;
  BlockWorkerGrpc.BlockWorkerBlockingStub mBlockingStub;
  BlockWorkerGrpc.BlockWorkerStub mAsyncStub;
  private static final long KEEPALIVE_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  final int mBlockSize;
  final int mChunkSize;

  public static class Builder {
    private final GrpcChannelBuilder mChannelBuilder;

    public Builder(GrpcChannelBuilder channelBuilder) {
      mChannelBuilder = channelBuilder;
    }

    public BlockWorkerClient build() {
      int chunkSize = 65535;
      long blockSizeBytes = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
      return new BlockWorkerClient(mChannelBuilder.build(), (int) blockSizeBytes, chunkSize);
    }
  }

  public static Builder getBuilder(Subject subject, SocketAddress address) {
    int flowControlWindow = 256000;
    return new Builder(GrpcChannelBuilder
        .forAddress(address)
        .channelType(NettyUtils.getClientChannelClass(
            !(address instanceof InetSocketAddress)))
        .usePlaintext(true)
        .keepAliveTimeout(KEEPALIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .maxInboundMessageSize(1024 * 1024 * 1024)
        .flowControlWindow(flowControlWindow));
  }

  public BlockWorkerClient(GrpcChannel channel, int blockSize, int chunkSize) {
    mChannel = channel;

    mBlockingStub = BlockWorkerGrpc.newBlockingStub(mChannel);
    mAsyncStub = BlockWorkerGrpc.newStub(mChannel);

    mBlockSize = blockSize;
    mChunkSize = chunkSize;

  }

  public boolean isHealthy() {
    ConnectivityState state = mChannel.getState(false);
    return state != ConnectivityState.SHUTDOWN;
  }

  @Override
  public void close() throws IOException {
    mChannel.shutdown();
    try {
      mChannel.awaitTermination(
          Configuration.getMs(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT),
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  public StreamObserver<WriteRequest> writeBlock(StreamObserver<WriteResponse> responseObserver) {
    return mAsyncStub.writeBlock(responseObserver);
  }

  public Iterator<ReadResponse> readBlock(final ReadRequest request) {
    return mBlockingStub.readBlock(request);
  }
}
