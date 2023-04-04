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

package alluxio.dora.client.block.stream;

import alluxio.dora.conf.AlluxioConfiguration;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.exception.status.AlluxioStatusException;
import alluxio.dora.exception.status.UnauthenticatedException;
import alluxio.dora.grpc.BlockWorkerGrpc;
import alluxio.dora.grpc.CacheRequest;
import alluxio.dora.grpc.ClearMetricsRequest;
import alluxio.dora.grpc.ClearMetricsResponse;
import alluxio.dora.grpc.CreateLocalBlockRequest;
import alluxio.dora.grpc.CreateLocalBlockResponse;
import alluxio.dora.grpc.DataMessageMarshaller;
import alluxio.dora.grpc.DataMessageMarshallerProvider;
import alluxio.dora.grpc.FreeWorkerRequest;
import alluxio.dora.grpc.GetStatusPRequest;
import alluxio.dora.grpc.GetStatusPResponse;
import alluxio.dora.grpc.GrpcChannel;
import alluxio.dora.grpc.GrpcChannelBuilder;
import alluxio.dora.grpc.GrpcNetworkGroup;
import alluxio.dora.grpc.GrpcSerializationUtils;
import alluxio.dora.grpc.GrpcServerAddress;
import alluxio.dora.grpc.ListStatusPRequest;
import alluxio.dora.grpc.ListStatusPResponse;
import alluxio.dora.grpc.LoadRequest;
import alluxio.dora.grpc.LoadResponse;
import alluxio.dora.grpc.MoveBlockRequest;
import alluxio.dora.grpc.MoveBlockResponse;
import alluxio.dora.grpc.OpenLocalBlockRequest;
import alluxio.dora.grpc.OpenLocalBlockResponse;
import alluxio.dora.grpc.ReadRequest;
import alluxio.dora.grpc.ReadResponse;
import alluxio.dora.grpc.RemoveBlockRequest;
import alluxio.dora.grpc.RemoveBlockResponse;
import alluxio.dora.grpc.WriteRequest;
import alluxio.dora.grpc.WriteResponse;
import alluxio.dora.resource.AlluxioResourceLeakDetectorFactory;
import alluxio.dora.retry.RetryPolicy;
import alluxio.dora.retry.RetryUtils;
import alluxio.dora.security.user.UserState;

import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link BlockWorkerClient}.
 */
public class DefaultBlockWorkerClient implements BlockWorkerClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultBlockWorkerClient.class.getName());

  private static final ResourceLeakDetector<DefaultBlockWorkerClient> DETECTOR =
      AlluxioResourceLeakDetectorFactory.instance()
          .newResourceLeakDetector(DefaultBlockWorkerClient.class);

  private GrpcChannel mStreamingChannel;
  private GrpcChannel mRpcChannel;
  private final GrpcServerAddress mAddress;
  private final long mRpcTimeoutMs;

  private final BlockWorkerGrpc.BlockWorkerStub mStreamingAsyncStub;
  private final BlockWorkerGrpc.BlockWorkerBlockingStub mRpcBlockingStub;
  private final BlockWorkerGrpc.BlockWorkerFutureStub mRpcFutureStub;

  @Nullable
  private final ResourceLeakTracker<DefaultBlockWorkerClient> mTracker;

  /**
   * Creates a client instance for communicating with block worker.
   *
   * @param userState     the user state
   * @param address     the address of the worker
   * @param alluxioConf Alluxio configuration
   */
  public DefaultBlockWorkerClient(UserState userState, GrpcServerAddress address,
      AlluxioConfiguration alluxioConf) throws IOException {
    RetryPolicy retryPolicy = RetryUtils.defaultClientRetry();
    UnauthenticatedException lastException = null;
    // TODO(feng): unify worker client with AbstractClient
    while (retryPolicy.attempt()) {
      try {
        // Disables channel pooling for data streaming to achieve better throughput.
        // Channel is still reused due to client pooling.
        mStreamingChannel = GrpcChannelBuilder.newBuilder(address, alluxioConf)
            .setSubject(userState.getSubject())
            .setNetworkGroup(GrpcNetworkGroup.STREAMING)
            .build();
        mStreamingChannel.intercept(new StreamSerializationClientInterceptor());
        // Uses default pooling strategy for RPC calls for better scalability.
        mRpcChannel = GrpcChannelBuilder.newBuilder(address, alluxioConf)
            .setSubject(userState.getSubject())
            .setNetworkGroup(GrpcNetworkGroup.RPC)
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
    mRpcFutureStub = BlockWorkerGrpc.newFutureStub(mRpcChannel);
    mAddress = address;
    mRpcTimeoutMs = alluxioConf.getMs(PropertyKey.USER_RPC_RETRY_MAX_DURATION);
    mTracker = DETECTOR.track(this);
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
      closer.register(() -> {
        if (mTracker != null) {
          mTracker.close(this);
        }
      });
    }
  }

  @Override
  public StreamObserver<WriteRequest> writeBlock(StreamObserver<WriteResponse> responseObserver) {
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
  }

  @Override
  public StreamObserver<ReadRequest> readBlock(StreamObserver<ReadResponse> responseObserver) {
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

  @Override
  public void freeWorker() {
    mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
            .freeWorker(FreeWorkerRequest.getDefaultInstance());
  }

  @Override
  public ListenableFuture<LoadResponse> load(LoadRequest request) {
    return mRpcFutureStub.load(request);
  }

  @Override
  public GetStatusPResponse getStatus(GetStatusPRequest request) {
    return mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .getStatus(request);
  }

  @Override
  public Iterator<ListStatusPResponse> listStatus(ListStatusPRequest request) {
    return mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .listStatus(request);
  }
}
