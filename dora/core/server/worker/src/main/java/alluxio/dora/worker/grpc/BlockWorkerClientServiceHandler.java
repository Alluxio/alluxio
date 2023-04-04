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

package alluxio.dora.worker.grpc;

import static java.util.Objects.requireNonNull;

import alluxio.dora.RpcUtils;
import alluxio.dora.annotation.SuppressFBWarnings;
import alluxio.dora.conf.Configuration;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.worker.block.DefaultBlockWorker;
import alluxio.dora.grpc.AsyncCacheRequest;
import alluxio.dora.grpc.AsyncCacheResponse;
import alluxio.dora.grpc.BlockStatus;
import alluxio.dora.grpc.BlockWorkerGrpc;
import alluxio.dora.grpc.CacheRequest;
import alluxio.dora.grpc.CacheResponse;
import alluxio.dora.grpc.ClearMetricsRequest;
import alluxio.dora.grpc.ClearMetricsResponse;
import alluxio.dora.grpc.CreateLocalBlockRequest;
import alluxio.dora.grpc.CreateLocalBlockResponse;
import alluxio.dora.grpc.FreeWorkerRequest;
import alluxio.dora.grpc.FreeWorkerResponse;
import alluxio.dora.grpc.LoadRequest;
import alluxio.dora.grpc.LoadResponse;
import alluxio.dora.grpc.MoveBlockRequest;
import alluxio.dora.grpc.MoveBlockResponse;
import alluxio.dora.grpc.OpenLocalBlockRequest;
import alluxio.dora.grpc.OpenLocalBlockResponse;
import alluxio.dora.grpc.ReadRequest;
import alluxio.dora.grpc.ReadResponse;
import alluxio.dora.grpc.ReadResponseMarshaller;
import alluxio.dora.grpc.RemoveBlockRequest;
import alluxio.dora.grpc.RemoveBlockResponse;
import alluxio.dora.grpc.TaskStatus;
import alluxio.dora.grpc.WriteRequestMarshaller;
import alluxio.dora.grpc.WriteResponse;
import alluxio.dora.security.authentication.AuthenticatedClientUser;
import alluxio.dora.security.authentication.AuthenticatedUserInfo;
import alluxio.dora.underfs.UfsManager;
import alluxio.dora.util.IdUtils;
import alluxio.dora.util.SecurityUtils;
import alluxio.dora.worker.block.AllocateOptions;
import alluxio.dora.worker.block.BlockStoreLocation;

import com.google.common.collect.ImmutableMap;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Server side implementation of the gRPC BlockWorker interface.
 */
@SuppressFBWarnings("BC_UNCONFIRMED_CAST")
public class BlockWorkerClientServiceHandler extends BlockWorkerGrpc.BlockWorkerImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerClientServiceHandler.class);
  private static final boolean ZERO_COPY_ENABLED =
      Configuration.getBoolean(PropertyKey.WORKER_NETWORK_ZEROCOPY_ENABLED);
  private final DefaultBlockWorker mBlockWorker;
  private final UfsManager mUfsManager;
  private final ReadResponseMarshaller mReadResponseMarshaller = new ReadResponseMarshaller();
  private final WriteRequestMarshaller mWriteRequestMarshaller = new WriteRequestMarshaller();
  private final boolean mDomainSocketEnabled;

  /**
   * Creates a new implementation of gRPC BlockWorker interface.
   *
   * @param blockWorker block worker
   * @param ufsManager ufs manager
   * @param domainSocketEnabled is using domain sockets
   */
  public BlockWorkerClientServiceHandler(DefaultBlockWorker blockWorker,
      UfsManager ufsManager,
      boolean domainSocketEnabled) {
    mBlockWorker = requireNonNull(blockWorker);
    mUfsManager = requireNonNull(ufsManager);
    mDomainSocketEnabled = domainSocketEnabled;
  }

  /**
   * @return a map of gRPC methods with overridden descriptors
   */
  public Map<MethodDescriptor, MethodDescriptor> getOverriddenMethodDescriptors() {
    if (ZERO_COPY_ENABLED) {
      return ImmutableMap.of(
          BlockWorkerGrpc.getReadBlockMethod(),
          BlockWorkerGrpc.getReadBlockMethod().toBuilder()
              .setResponseMarshaller(mReadResponseMarshaller).build(),
          BlockWorkerGrpc.getWriteBlockMethod(),
          BlockWorkerGrpc.getWriteBlockMethod().toBuilder()
              .setRequestMarshaller(mWriteRequestMarshaller).build());
    }
    return Collections.emptyMap();
  }

  @Override
  public StreamObserver<ReadRequest> readBlock(StreamObserver<ReadResponse> responseObserver) {
    CallStreamObserver<ReadResponse> callStreamObserver =
        (CallStreamObserver<ReadResponse>) responseObserver;
    if (ZERO_COPY_ENABLED) {
      callStreamObserver =
          new DataMessageServerStreamObserver<>(callStreamObserver, mReadResponseMarshaller);
    }
    BlockReadHandler readHandler = new BlockReadHandler(GrpcExecutors.BLOCK_READER_EXECUTOR,
        mBlockWorker, callStreamObserver, mDomainSocketEnabled);
    callStreamObserver.setOnReadyHandler(readHandler::onReady);
    return readHandler;
  }

  @Override
  public StreamObserver<alluxio.dora.grpc.WriteRequest> writeBlock(
      StreamObserver<WriteResponse> responseObserver) {
    ServerCallStreamObserver<WriteResponse> serverResponseObserver =
        (ServerCallStreamObserver<WriteResponse>) responseObserver;
    if (ZERO_COPY_ENABLED) {
      responseObserver =
          new DataMessageServerRequestObserver<>(responseObserver, mWriteRequestMarshaller, null);
    }
    DelegationWriteHandler handler = new DelegationWriteHandler(mBlockWorker, mUfsManager,
        responseObserver, getAuthenticatedUserInfo(), mDomainSocketEnabled);
    serverResponseObserver.setOnCancelHandler(handler::onCancel);
    return handler;
  }

  @Override
  public StreamObserver<OpenLocalBlockRequest> openLocalBlock(
      StreamObserver<OpenLocalBlockResponse> responseObserver) {
    return new ShortCircuitBlockReadHandler(
        mBlockWorker.getBlockStore(), responseObserver);
  }

  @Override
  public StreamObserver<CreateLocalBlockRequest> createLocalBlock(
      StreamObserver<CreateLocalBlockResponse> responseObserver) {
    ShortCircuitBlockWriteHandler handler = new ShortCircuitBlockWriteHandler(
        mBlockWorker, responseObserver);
    ServerCallStreamObserver<CreateLocalBlockResponse> serverCallStreamObserver =
        (ServerCallStreamObserver<CreateLocalBlockResponse>) responseObserver;
    serverCallStreamObserver.setOnCancelHandler(handler::onCancel);
    return handler;
  }

  @Override
  public void asyncCache(AsyncCacheRequest request,
      StreamObserver<AsyncCacheResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mBlockWorker.asyncCache(request);
      return AsyncCacheResponse.getDefaultInstance();
    }, "asyncCache", "request=%s", responseObserver, request);
  }

  @Override
  public void cache(CacheRequest request, StreamObserver<CacheResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mBlockWorker.cache(request);
      return CacheResponse.getDefaultInstance();
    }, "cache", "request=%s", responseObserver, request);
  }

  @Override
  public void load(LoadRequest request, StreamObserver<LoadResponse> responseObserver) {
    CompletableFuture<List<BlockStatus>> failures =
        mBlockWorker.load(request.getBlocksList(), request.getOptions());
    CompletableFuture<LoadResponse> future = failures.thenApply(fail -> {
      int numBlocks = request.getBlocksCount();
      TaskStatus taskStatus = TaskStatus.SUCCESS;
      if (fail.size() > 0) {
        taskStatus = numBlocks > fail.size() ? TaskStatus.PARTIAL_FAILURE : TaskStatus.FAILURE;
      }
      LoadResponse.Builder response = LoadResponse.newBuilder();
      return response.addAllBlockStatus(fail).setStatus(taskStatus).build();
    });
    RpcUtils.invoke(LOG, future, "load", "request=%s", responseObserver, request);
  }

  @Override
  public void removeBlock(RemoveBlockRequest request,
      StreamObserver<RemoveBlockResponse> responseObserver) {
    long sessionId = IdUtils.createSessionId();
    RpcUtils.call(LOG, () -> {
      mBlockWorker.removeBlock(sessionId, request.getBlockId());
      return RemoveBlockResponse.getDefaultInstance();
    }, "removeBlock", "request=%s", responseObserver, request);
  }

  @Override
  public void moveBlock(MoveBlockRequest request,
      StreamObserver<MoveBlockResponse> responseObserver) {
    long sessionId = IdUtils.createSessionId();
    RpcUtils.call(LOG, () -> {
      mBlockWorker.getBlockStore()
          .moveBlock(sessionId, request.getBlockId(),
              AllocateOptions.forMove(
                  BlockStoreLocation.anyDirInAnyTierWithMedium(request.getMediumType())));
      return MoveBlockResponse.getDefaultInstance();
    }, "moveBlock", "request=%s", responseObserver, request);
  }

  @Override
  public void freeWorker(FreeWorkerRequest request,
       StreamObserver<FreeWorkerResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mBlockWorker.freeWorker();
      return FreeWorkerResponse.getDefaultInstance();
    }, "freeWorker", "request=%s", responseObserver, request);
  }

  @Override
  public void clearMetrics(ClearMetricsRequest request,
      StreamObserver<ClearMetricsResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mBlockWorker.clearMetrics();
      return ClearMetricsResponse.getDefaultInstance();
    }, "clearMetrics", "request=%s", responseObserver, request);
  }

  /**
   * @return {@link AuthenticatedUserInfo} that defines the user that has been authorized
   */
  private AuthenticatedUserInfo getAuthenticatedUserInfo() {
    try {
      if (SecurityUtils.isAuthenticationEnabled(Configuration.global())) {
        return new AuthenticatedUserInfo(
            AuthenticatedClientUser.getClientUser(Configuration.global()),
            AuthenticatedClientUser.getConnectionUser(Configuration.global()),
            AuthenticatedClientUser.getAuthMethod(Configuration.global()));
      } else {
        return new AuthenticatedUserInfo();
      }
    } catch (Exception e) {
      throw Status.UNAUTHENTICATED.withDescription(e.toString()).asRuntimeException();
    }
  }
}
