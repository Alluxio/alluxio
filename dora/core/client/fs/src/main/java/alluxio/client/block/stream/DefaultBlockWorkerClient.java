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
import alluxio.grpc.CompleteFilePRequest;
import alluxio.grpc.CompleteFilePResponse;
import alluxio.grpc.CopyRequest;
import alluxio.grpc.CopyResponse;
import alluxio.grpc.CreateDirectoryPRequest;
import alluxio.grpc.CreateDirectoryPResponse;
import alluxio.grpc.CreateFilePRequest;
import alluxio.grpc.CreateFilePResponse;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.DataMessageMarshaller;
import alluxio.grpc.DataMessageMarshallerProvider;
import alluxio.grpc.DeletePRequest;
import alluxio.grpc.DeletePResponse;
import alluxio.grpc.FreeWorkerRequest;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcNetworkGroup;
import alluxio.grpc.GrpcSerializationUtils;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.ListStatusPResponse;
import alluxio.grpc.LoadFileRequest;
import alluxio.grpc.LoadFileResponse;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.MoveBlockRequest;
import alluxio.grpc.MoveBlockResponse;
import alluxio.grpc.MoveRequest;
import alluxio.grpc.MoveResponse;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.RenamePRequest;
import alluxio.grpc.RenamePResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.resource.AlluxioResourceLeakDetectorFactory;
import alluxio.resource.LockResource;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.security.user.UserState;

import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
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

  protected DefaultBlockWorkerClient(UserState userState, GrpcServerAddress address,
      AlluxioConfiguration alluxioConf, GrpcChannel streamingChannel, GrpcChannel rpcChannel)
      throws IOException {
    mStreamingChannel = streamingChannel;
    mRpcChannel = rpcChannel;
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

  /**
   * No data read stream observer.
   */
  public static class NoDataReadStreamObserver
      implements StreamObserver<alluxio.grpc.ReadResponse> {

    SettableFuture<Object> mFuture = SettableFuture.create();
    ReentrantLock mLock = new ReentrantLock();

    @Override
    public void onNext(ReadResponse response) {
      // I don't care
    }

    @Override
    public void onError(Throwable t) {
      try (LockResource ignored = new LockResource(mLock)) {
        LOG.warn("onError : {}", t);
        mFuture.setException(t);
      }
    }

    @Override
    public void onCompleted() {
      try (LockResource ignored = new LockResource(mLock)) {
        LOG.info("onComplete.");
        mFuture.set(true);
      }
    }

    /**
     * @return future
     */
    public ListenableFuture<Object> getFuture() {
      return mFuture;
    }
  }

  @Override
  public ListenableFuture<Object> readBlockNoDataBack(ReadRequest request) {
    NoDataReadStreamObserver responseStreamObserver = new NoDataReadStreamObserver();
    StreamObserver<ReadRequest> requestStreamObserver = mStreamingAsyncStub
        .readBlock(responseStreamObserver);
    requestStreamObserver.onNext(request);
    requestStreamObserver.onCompleted();
    return responseStreamObserver.getFuture();
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
  public ListenableFuture<LoadFileResponse> loadFile(LoadFileRequest request) {
    return mRpcFutureStub.loadFile(request);
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

  @Override
  public ListenableFuture<CopyResponse> copy(CopyRequest request) {
    return mRpcFutureStub.copy(request);
  }

  @Override
  public ListenableFuture<MoveResponse> move(MoveRequest request) {
    return mRpcFutureStub.move(request);
  }

  @Override
  public CreateFilePResponse createFile(CreateFilePRequest request) {
    return mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .createFile(request);
  }

  @Override
  public CompleteFilePResponse completeFile(CompleteFilePRequest request) {
    return mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .completeFile(request);
  }

  @Override
  public DeletePResponse delete(DeletePRequest request) {
    return mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .remove(request);
  }

  @Override
  public RenamePResponse rename(RenamePRequest request) {
    return mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .rename(request);
  }

  @Override
  public CreateDirectoryPResponse createDirectory(CreateDirectoryPRequest request) {
    return mRpcBlockingStub.withDeadlineAfter(mRpcTimeoutMs, TimeUnit.MILLISECONDS)
        .createDirectory(request);
  }
}
