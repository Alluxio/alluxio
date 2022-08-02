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

import alluxio.AbstractClient;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.ClearMetricsRequest;
import alluxio.grpc.ClearMetricsResponse;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.DataMessageMarshaller;
import alluxio.grpc.DataMessageMarshallerProvider;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcNetworkGroup;
import alluxio.grpc.GrpcSerializationUtils;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.MoveBlockRequest;
import alluxio.grpc.MoveBlockResponse;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.ServiceType;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.retry.CountingRetry;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Default implementation of {@link BlockWorkerClient}.
 */
public class DefaultBlockWorkerClient extends AbstractClient implements BlockWorkerClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultBlockWorkerClient.class.getName());

  /**Streaming channel used for streaming rpc calls. */
  private GrpcChannel mStreamingChannel;
  private final GrpcServerAddress mAddress;
  private final long mRpcTimeoutMs;

  private BlockWorkerGrpc.BlockWorkerStub mStreamingAsyncStub;
  private BlockWorkerGrpc.BlockWorkerBlockingStub mRpcBlockingStub;
  private BlockWorkerGrpc.BlockWorkerFutureStub mRpcFutureStub;

  /**
   * Creates a client instance for communicating with block worker.
   *
   * @param context     the client context
   * @param address     the address of the worker
   * @param alluxioConf Alluxio configuration
   */
  public DefaultBlockWorkerClient(ClientContext context, GrpcServerAddress address,
                                  AlluxioConfiguration alluxioConf) {
    // BlockWorkerClient is typically used inside AlluxioFileInStream
    // to fetch block chunk. AlluxioFilInStream has its own retry mechanism, so we don't
    // retry at this level so as not to interfere with the retry of our higher-level
    // callers.
    // An instance where retries interfere with each other is when we and our calling
    // AlluxioFileInStream both adopted a timer-based retry, then all the time could be wasted
    // in our inner retry, and the outer stream has no chance of retrying a different client.
    super(context, () -> new CountingRetry(0));
    // the server address this client connects to. It might not be an IP address
    mAddress = address;
    mRpcTimeoutMs = alluxioConf.getMs(PropertyKey.USER_RPC_RETRY_MAX_DURATION);
  }

  @Override
  protected void beforeConnect() {}

  @Override
  protected void afterConnect() {
    // set up stubs
    mStreamingAsyncStub = BlockWorkerGrpc.newStub(mStreamingChannel);
    mRpcFutureStub = BlockWorkerGrpc.newFutureStub(mChannel);
    mRpcBlockingStub = BlockWorkerGrpc.newBlockingStub(mChannel);
  }

  @Override
  protected void doConnect() throws AlluxioStatusException {
    super.doConnect();

    // build a streaming channel
    AlluxioConfiguration conf = mContext.getClusterConf();
    mStreamingChannel = GrpcChannelBuilder.newBuilder(mServerAddress, conf)
            .setSubject(mContext.getSubject())
            .setNetworkGroup(GrpcNetworkGroup.STREAMING)
            .build();
    mStreamingChannel.intercept(new StreamSerializationClientInterceptor());
  }

  @Override
  protected void shutdownChannelsIfCreated() {
    super.shutdownChannelsIfCreated();
    if (mStreamingChannel != null) {
      mStreamingChannel.shutdown();
      mStreamingChannel = null;
    }
  }

  @Override
  public boolean isShutdown() {
    return isClosed();
  }

  @Override
  public boolean isHealthy() {
    if (isShutdown()) {
      // client is shut down
      return false;
    }

    if (isConnected()) {
      return mChannel.isHealthy() && mStreamingChannel.isHealthy();
    }

    // client is not connected yet, assume it is healthy
    return true;
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.BLOCK_WORKER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected GrpcServerAddress queryGrpcServerAddress() {
    return mAddress;
  }

  @Override
  public StreamObserver<WriteRequest> writeBlock(StreamObserver<WriteResponse> responseObserver)
          throws AlluxioStatusException {
    return retryRPC(() -> {
      if (responseObserver instanceof DataMessageMarshallerProvider) {
        DataMessageMarshaller<WriteRequest> marshaller =
                ((DataMessageMarshallerProvider<WriteRequest, WriteResponse>) responseObserver)
                        .getRequestMarshaller().orElseThrow(NullPointerException::new);
        return mStreamingAsyncStub
                .withOption(GrpcSerializationUtils.OVERRIDDEN_METHOD_DESCRIPTOR,
                        BlockWorkerGrpc.getWriteBlockMethod().toBuilder()
                                .setRequestMarshaller(marshaller)
                                .build())
                .writeBlock(responseObserver);
      } else {
        return mStreamingAsyncStub.writeBlock(responseObserver);
      }
    }, LOG, "writeBlock", "write block");
  }

  @Override
  public StreamObserver<ReadRequest> readBlock(StreamObserver<ReadResponse> responseObserver)
          throws AlluxioStatusException {
    return retryRPC(() -> {
      if (responseObserver instanceof DataMessageMarshallerProvider) {
        DataMessageMarshaller<ReadResponse> marshaller =
                ((DataMessageMarshallerProvider<ReadRequest, ReadResponse>) responseObserver)
                        .getResponseMarshaller().orElseThrow(NullPointerException::new);
        return mStreamingAsyncStub
                .withOption(GrpcSerializationUtils.OVERRIDDEN_METHOD_DESCRIPTOR,
                        BlockWorkerGrpc.getReadBlockMethod().toBuilder()
                                .setResponseMarshaller(marshaller)
                                .build())
                .readBlock(responseObserver);
      } else {
        return mStreamingAsyncStub.readBlock(responseObserver);
      }
    }, LOG, "readBlock", "read block");
  }

  @Override
  public StreamObserver<CreateLocalBlockRequest> createLocalBlock(
      StreamObserver<CreateLocalBlockResponse> responseObserver) throws AlluxioStatusException {
    return retryRPC(() -> mStreamingAsyncStub.createLocalBlock(responseObserver),
            LOG, "createLocalBlock", "create local block");
  }

  @Override
  public StreamObserver<OpenLocalBlockRequest> openLocalBlock(
      StreamObserver<OpenLocalBlockResponse> responseObserver) throws AlluxioStatusException {
    return retryRPC(() -> mStreamingAsyncStub.openLocalBlock(responseObserver),
            LOG, "openLocalBlock", "open local block");
  }

  @Override
  public RemoveBlockResponse removeBlock(final RemoveBlockRequest request)
          throws AlluxioStatusException {
    return retryRPC(() -> mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .removeBlock(request), LOG, "removeBlock", "remove block");
  }

  @Override
  public MoveBlockResponse moveBlock(MoveBlockRequest request)
          throws AlluxioStatusException {
    return retryRPC(() -> mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
            .moveBlock(request), LOG, "moveBlock", "move block");
  }

  @Override
  public ClearMetricsResponse clearMetrics(ClearMetricsRequest request)
          throws AlluxioStatusException {
    return retryRPC(() -> mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .clearMetrics(request), LOG, "clearMetric", "clear metric");
  }

  @Override
  public void cache(CacheRequest request) throws AlluxioStatusException {
    retryRPC(() -> {
      boolean async = request.getAsync();
      try {
        mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS).cache(request);
      } catch (Exception e) {
        if (!async) {
          throw e;
        }
        LOG.warn("Error sending async cache request {} to worker {}.", request, mAddress, e);
      }
      return null;
    }, LOG, "cache", "cache block");
  }

  @Override
  public ListenableFuture<LoadResponse> load(LoadRequest request) throws AlluxioStatusException {
    return retryRPC(() -> mRpcFutureStub.load(request), LOG, "load", "load block");
  }
}
