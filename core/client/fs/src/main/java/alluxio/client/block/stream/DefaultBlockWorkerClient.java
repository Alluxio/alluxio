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

import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.AsyncCacheResponse;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.GrpcManagedChannelPool;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NettyUtils;

import com.google.common.io.Closer;
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
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultBlockWorkerClient.class.getName());

  // TODO(zac): Make this a non-singleton
  private static final EventLoopGroup WORKER_GROUP = NettyUtils
      .createEventLoop(
          NettyUtils.getUserChannel(new InstancedConfiguration(ConfigurationUtils.defaults())),
          new InstancedConfiguration(ConfigurationUtils.defaults())
              .getInt(PropertyKey.USER_NETWORK_NETTY_WORKER_THREADS),
          "netty-client-worker-%d", true);

  private GrpcChannel mStreamingChannel;
  private GrpcChannel mRpcChannel;
  private SocketAddress mAddress;
  private final long mDataTimeoutMs;

  private BlockWorkerGrpc.BlockWorkerStub mStreamingAsyncStub;
  private BlockWorkerGrpc.BlockWorkerBlockingStub mRpcBlockingStub;
  private BlockWorkerGrpc.BlockWorkerStub mRpcAsyncStub;

  /**
   * Creates a client instance for communicating with block worker.
   *
   * @param subject the user subject, can be null if the user is not available
   * @param address the address of the worker
   * @param alluxioConf Alluxio configuration
   */
  public DefaultBlockWorkerClient(Subject subject, SocketAddress address,
      AlluxioConfiguration alluxioConf) throws IOException {
    try {
      // Disables channel pooling for data streaming to achieve better throughput.
      // Channel is still reused due to client pooling.
      mStreamingChannel = buildChannel(subject, address,
          GrpcManagedChannelPool.PoolingStrategy.DISABLED, alluxioConf);
      // Uses default pooling strategy for RPC calls for better scalability.
      mRpcChannel = buildChannel(subject, address,
          GrpcManagedChannelPool.PoolingStrategy.DEFAULT, alluxioConf);
    } catch (StatusRuntimeException e) {
      throw GrpcExceptionUtils.fromGrpcStatusException(e);
    }
    mStreamingAsyncStub = BlockWorkerGrpc.newStub(mStreamingChannel);
    mRpcBlockingStub = BlockWorkerGrpc.newBlockingStub(mRpcChannel);
    mRpcAsyncStub = BlockWorkerGrpc.newStub(mRpcChannel);
    mAddress = address;
    mDataTimeoutMs = alluxioConf.getMs(PropertyKey.USER_NETWORK_DATA_TIMEOUT_MS);
  }

  @Override
  public boolean isShutdown() {
    return mStreamingChannel.isShutdown() || mRpcChannel.isShutdown();
  }

  @Override
  public boolean isHealthy() {
    return !isShutdown() && mStreamingChannel.isHealthy() && mRpcChannel.isHealthy();
  }

  @Override
  public void close() throws IOException {
    try (Closer closer = Closer.create()) {
      closer.register(() -> mStreamingChannel.shutdown());
      closer.register(() -> mRpcChannel.shutdown());
    }
  }

  @Override
  public StreamObserver<WriteRequest> writeBlock(StreamObserver<WriteResponse> responseObserver) {
    return mStreamingAsyncStub.writeBlock(responseObserver);
  }

  @Override
  public StreamObserver<ReadRequest> readBlock(StreamObserver<ReadResponse> responseObserver) {
    return mStreamingAsyncStub.readBlock(responseObserver);
  }

  @Override
  public StreamObserver<CreateLocalBlockRequest> createLocalBlock(
      StreamObserver<CreateLocalBlockResponse> responseObserver) {
    return mStreamingAsyncStub.createLocalBlock(responseObserver);
  }

  @Override
  public StreamObserver<OpenLocalBlockRequest> openLocalBlock(
      StreamObserver<OpenLocalBlockResponse> responseObserver) {
    return mStreamingAsyncStub.openLocalBlock(responseObserver);
  }

  @Override
  public RemoveBlockResponse removeBlock(final RemoveBlockRequest request) {
    return mRpcBlockingStub.withDeadlineAfter(mDataTimeoutMs, TimeUnit.MILLISECONDS)
        .removeBlock(request);
  }

  @Override
  public void asyncCache(final AsyncCacheRequest request) {
    mRpcAsyncStub.withDeadlineAfter(mDataTimeoutMs, TimeUnit.MILLISECONDS)
        .asyncCache(request, new StreamObserver<AsyncCacheResponse>() {
          @Override
          public void onNext(AsyncCacheResponse value) {
            // we don't use response from the RPC
          }

          @Override
          public void onError(Throwable t) {
            LOG.warn("Error sending async cache request {} to worker {}.", request, mAddress, t);
          }

          @Override
          public void onCompleted() {
            // we don't use response from the RPC
          }
        });
  }

  private GrpcChannel buildChannel(Subject subject, SocketAddress address,
      GrpcManagedChannelPool.PoolingStrategy poolingStrategy, AlluxioConfiguration alluxioConf)
      throws UnauthenticatedException, UnavailableException {
    return GrpcChannelBuilder.newBuilder(address, alluxioConf).setSubject(subject)
        .setChannelType(NettyUtils
            .getClientChannelClass(!(address instanceof InetSocketAddress), alluxioConf))
        .setPoolingStrategy(poolingStrategy)
        .setEventLoopGroup(WORKER_GROUP)
        .setKeepAliveTimeout(alluxioConf.getMs(PropertyKey.USER_NETWORK_KEEPALIVE_TIMEOUT_MS),
            TimeUnit.MILLISECONDS)
        .setMaxInboundMessageSize(
            (int) alluxioConf.getBytes(PropertyKey.USER_NETWORK_MAX_INBOUND_MESSAGE_SIZE))
        .setFlowControlWindow(
            (int) alluxioConf.getBytes(PropertyKey.USER_NETWORK_FLOWCONTROL_WINDOW))
        .build();
  }
}
