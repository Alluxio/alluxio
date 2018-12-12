/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block.stream;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.util.grpc.GrpcChannel;
import alluxio.util.grpc.GrpcChannelBuilder;
import alluxio.util.network.NettyUtils;

import io.grpc.ConnectivityState;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

/**
 * Default implementation of {@link BlockWorkerClient}.
 */
public class DefaultBlockWorkerClient implements BlockWorkerClient {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DefaultBlockWorkerClient.class.getName());

  private static final long KEEPALIVE_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
  private static final long GRPC_FLOWCONTROL_WINDOW =
      Configuration.getMs(PropertyKey.USER_NETWORK_GRPC_FLOWCONTROL_WINDOW);
  private static final long MAX_INBOUND_MESSAGE_SIZE =
      Configuration.getMs(PropertyKey.USER_NETWORK_GRPC_MAX_INBOUND_MESSAGE_SIZE);
  private static final EventLoopGroup WORKER_GROUP = NettyUtils
      .createEventLoop(NettyUtils.USER_CHANNEL_TYPE,
          Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_WORKER_THREADS),
          "netty-client-worker-%d", true);

  private GrpcChannel mChannel;
  private BlockWorkerGrpc.BlockWorkerBlockingStub mBlockingStub;
  private BlockWorkerGrpc.BlockWorkerStub mAsyncStub;

  /**
   * Builder for {@link DefaultBlockWorkerClient}.
   */
  public static class Builder implements BlockWorkerClient.Builder {
    private final GrpcChannelBuilder mChannelBuilder;

    /**
     * Creates a {@link Builder} for {@link DefaultBlockWorkerClient}.
     * @param channelBuilder a gRPC channel builder
     */
    public Builder(GrpcChannelBuilder channelBuilder) {
      mChannelBuilder = channelBuilder;
    }

    @Override
    public BlockWorkerClient build() {
      return new DefaultBlockWorkerClient(mChannelBuilder.build());
    }
  }

  /**
   * Gets a builder for given user subject and address.
   *
   * @param subject the user subject
   * @param address the address of the worker
   * @return the builder for the client
   */
  public static Builder getBuilder(Subject subject, SocketAddress address) {
    return new Builder(GrpcChannelBuilder
        .forAddress(address)
        .channelType(NettyUtils.getClientChannelClass(
            !(address instanceof InetSocketAddress)))
        .group(WORKER_GROUP)
        .usePlaintext(true)
        .keepAliveTimeout(KEEPALIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .maxInboundMessageSize((int) MAX_INBOUND_MESSAGE_SIZE)
        .flowControlWindow((int) GRPC_FLOWCONTROL_WINDOW));
  }

  /**
   * Creates a client instance for communicating with block worker.
   *
   * @param channel the gRPC channel
   */
  public DefaultBlockWorkerClient(GrpcChannel channel) {
    mChannel = channel;
    mBlockingStub = BlockWorkerGrpc.newBlockingStub(mChannel);
    mAsyncStub = BlockWorkerGrpc.newStub(mChannel);
  }

  @Override
  public boolean isShutdown() {
    ConnectivityState state = mChannel.getState(false);
    return state == ConnectivityState.SHUTDOWN;
  }

  @Override
  public void close() throws IOException {
    mChannel.shutdown();
    try {
      if (!mChannel.awaitTermination(
          Configuration.getMs(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT),
          TimeUnit.MILLISECONDS)) {
        LOGGER.warn("Failed to close gRPC channel for block worker.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public StreamObserver<WriteRequest> writeBlock(StreamObserver<WriteResponse> responseObserver) {
    return mAsyncStub.writeBlock(responseObserver);
  }

  @Override
  public Iterator<ReadResponse> readBlock(final ReadRequest request) {
    return mBlockingStub.readBlock(request);
  }
}
