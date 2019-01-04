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
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.AsyncCacheResponse;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.util.network.NettyUtils;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

/**
 * Default implementation of {@link BlockWorkerClient}.
 */
public class DefaultBlockWorkerClient implements BlockWorkerClient {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DefaultBlockWorkerClient.class.getName());

  private static final long KEEPALIVE_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_KEEPALIVE_TIMEOUT_MS);
  private static final long GRPC_FLOWCONTROL_WINDOW =
      Configuration.getBytes(PropertyKey.USER_NETWORK_FLOWCONTROL_WINDOW);
  private static final long MAX_INBOUND_MESSAGE_SIZE =
      Configuration.getBytes(PropertyKey.USER_NETWORK_MAX_INBOUND_MESSAGE_SIZE);
  private static final EventLoopGroup WORKER_GROUP = NettyUtils
      .createEventLoop(NettyUtils.USER_CHANNEL_TYPE,
          Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_WORKER_THREADS),
          "netty-client-worker-%d", true);

  private GrpcChannel mChannel;
  private SocketAddress mAddress;
  private BlockWorkerGrpc.BlockWorkerBlockingStub mBlockingStub;
  private BlockWorkerGrpc.BlockWorkerStub mAsyncStub;

  /**
   * Creates a client instance for communicating with block worker.
   *
   * @param subject the user subject, can be null if the user is not available
   * @param address the address of the worker
   */
  public DefaultBlockWorkerClient(Subject subject, SocketAddress address) throws IOException {
    try {
      mChannel = GrpcChannelBuilder.forAddress(address).setSubject(subject)
          .setChannelType(NettyUtils.getClientChannelClass(!(address instanceof InetSocketAddress)))
          .setEventLoopGroup(WORKER_GROUP)
          .setKeepAliveTimeout(KEEPALIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
          .setMaxInboundMessageSize((int) MAX_INBOUND_MESSAGE_SIZE)
          .setFlowControlWindow((int) GRPC_FLOWCONTROL_WINDOW).build();
    } catch (StatusRuntimeException e) {
      throw GrpcExceptionUtils.fromGrpcStatusException(e);
    }
    mBlockingStub = BlockWorkerGrpc.newBlockingStub(mChannel);
    mAsyncStub = BlockWorkerGrpc.newStub(mChannel);
    mAddress = address;
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
    return mChannel.isShutdown();
  }

  @Override
  public void close() throws IOException {
    mChannel.shutdown();
  }

  @Override
  public StreamObserver<WriteRequest> writeBlock(StreamObserver<WriteResponse> responseObserver) {
    return mAsyncStub.writeBlock(responseObserver);
  }

  @Override
  public StreamObserver<ReadRequest> readBlock(StreamObserver<ReadResponse> responseObserver) {
    return mAsyncStub.readBlock(responseObserver);
  }

  @Override
  public StreamObserver<CreateLocalBlockRequest> createLocalBlock(
      StreamObserver<CreateLocalBlockResponse> responseObserver) {
    return mAsyncStub.createLocalBlock(responseObserver);
  }

  @Override
  public StreamObserver<OpenLocalBlockRequest> openLocalBlock(
      StreamObserver<OpenLocalBlockResponse> responseObserver) {
    return mAsyncStub.openLocalBlock(responseObserver);
  }

  @Override
  public RemoveBlockResponse removeBlock(final RemoveBlockRequest request) {
    return mBlockingStub.withDeadlineAfter(
        Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS), TimeUnit.MILLISECONDS)
        .removeBlock(request);
  }

  @Override
  public void asyncCache(final AsyncCacheRequest request) {
    mAsyncStub.withDeadlineAfter(Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS),
        TimeUnit.MILLISECONDS).asyncCache(request, new StreamObserver<AsyncCacheResponse>() {
          @Override
          public void onNext(AsyncCacheResponse value) {
            // we don't use response from the RPC
          }

          @Override
          public void onError(Throwable t) {
            LOGGER.warn("Error sending async cache request {} to worker {}.", request, mAddress, t);
          }

          @Override
          public void onCompleted() {
            // we don't use response from the RPC
          }
        });
  }
}
