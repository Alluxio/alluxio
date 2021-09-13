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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnauthenticatedException;
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
import alluxio.grpc.MoveBlockRequest;
import alluxio.grpc.MoveBlockResponse;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.security.user.UserState;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of {@link BlockWorkerClient}.
 */
public class DefaultBlockWorkerClient implements BlockWorkerClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultBlockWorkerClient.class.getName());

  private GrpcChannel mStreamingChannel;
  private GrpcChannel mRpcChannel;
  private GrpcServerAddress mAddress;
  private final long mRpcTimeoutMs;

  private BlockWorkerGrpc.BlockWorkerStub mStreamingAsyncStub;
  private BlockWorkerGrpc.BlockWorkerBlockingStub mRpcBlockingStub;
  private BlockWorkerGrpc.BlockWorkerStub mRpcAsyncStub;

  /**
   * Creates a client instance for communicating with block worker.
   *
   * @param userState     the user state
   * @param address     the address of the worker
   * @param alluxioConf Alluxio configuration
   */
  public DefaultBlockWorkerClient(UserState userState, GrpcServerAddress address,
      AlluxioConfiguration alluxioConf) throws IOException {
    RetryPolicy retryPolicy = RetryUtils.defaultClientRetry(
        alluxioConf.getDuration(PropertyKey.USER_RPC_RETRY_MAX_DURATION),
        alluxioConf.getDuration(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS),
        alluxioConf.getDuration(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS));
    UnauthenticatedException lastException = null;
    // TODO(feng): unify worker client with AbstractClient
    while (retryPolicy.attempt()) {
      try {
        // Disables channel pooling for data streaming to achieve better throughput.
        // Channel is still reused due to client pooling.
        mStreamingChannel = GrpcChannelBuilder.newBuilder(address, alluxioConf)
            .setSubject(userState.getSubject())
            .setNetworkGroup(GrpcNetworkGroup.STREAMING)
            .setClientType("DefaultBlockWorkerClient-Stream")
            .build();
        mStreamingChannel.intercept(new StreamSerializationClientInterceptor());
        // Uses default pooling strategy for RPC calls for better scalability.
        mRpcChannel = GrpcChannelBuilder.newBuilder(address, alluxioConf)
            .setSubject(userState.getSubject())
            .setNetworkGroup(GrpcNetworkGroup.RPC)
            .setClientType("DefaultBlockWorkerClient-Rpc")
            .build();
        lastException = null;
        break;
      } catch (StatusRuntimeException e) {
        close();
        throw AlluxioStatusException.fromStatusRuntimeException(e);
      } catch (UnauthenticatedException e) {
        close();
        userState.relogin();
        lastException = e;
      }
    }
    if (lastException != null) {
      throw lastException;
    }
    mStreamingAsyncStub = BlockWorkerGrpc.newStub(mStreamingChannel);
    mRpcBlockingStub = BlockWorkerGrpc.newBlockingStub(mRpcChannel);
    mRpcAsyncStub = BlockWorkerGrpc.newStub(mRpcChannel);
    mAddress = address;
    mRpcTimeoutMs = alluxioConf.getMs(PropertyKey.USER_RPC_RETRY_MAX_DURATION);
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
      closer.register(() -> {
        if (mStreamingChannel != null) {
          mStreamingChannel.shutdown();
        }
      });
      closer.register(() -> {
        if (mRpcChannel != null) {
          mRpcChannel.shutdown();
        }
      });
    }
  }

  @Override
  public StreamObserver<WriteRequest> writeBlock(StreamObserver<WriteResponse> responseObserver) {
    if (responseObserver instanceof DataMessageMarshallerProvider) {
      DataMessageMarshaller<WriteRequest> marshaller =
          ((DataMessageMarshallerProvider<WriteRequest, WriteResponse>) responseObserver)
              .getRequestMarshaller();
      Preconditions.checkNotNull(marshaller, "marshaller");
      return mStreamingAsyncStub
          .withOption(GrpcSerializationUtils.OVERRIDDEN_METHOD_DESCRIPTOR,
              BlockWorkerGrpc.getWriteBlockMethod().toBuilder()
                  .setRequestMarshaller(marshaller)
                  .build())
          .writeBlock(responseObserver);
    } else {
      return mStreamingAsyncStub.writeBlock(responseObserver);
    }
  }

  @Override
  public StreamObserver<ReadRequest> readBlock(StreamObserver<ReadResponse> responseObserver) {
    if (responseObserver instanceof DataMessageMarshallerProvider) {
      DataMessageMarshaller<ReadResponse> marshaller =
          ((DataMessageMarshallerProvider<ReadRequest, ReadResponse>) responseObserver)
              .getResponseMarshaller();
      Preconditions.checkNotNull(marshaller);
      return mStreamingAsyncStub
          .withOption(GrpcSerializationUtils.OVERRIDDEN_METHOD_DESCRIPTOR,
              BlockWorkerGrpc.getReadBlockMethod().toBuilder()
                  .setResponseMarshaller(marshaller)
                  .build())
          .readBlock(responseObserver);
    } else {
      return mStreamingAsyncStub.readBlock(responseObserver);
    }
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
    return mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .removeBlock(request);
  }

  @Override
  public MoveBlockResponse moveBlock(MoveBlockRequest request) {
    return mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .moveBlock(request);
  }

  @Override
  public ClearMetricsResponse clearMetrics(ClearMetricsRequest request) {
    return mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .clearMetrics(request);
  }

  @Override
  public void cache(CacheRequest request) {
    boolean async = request.getAsync();
    try {
      mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS).cache(request);
    } catch (Exception e) {
      if (!async) {
        throw e;
      }
      LOG.warn("Error sending async cache request {} to worker {}.", request, mAddress, e);
    }
  }
}
