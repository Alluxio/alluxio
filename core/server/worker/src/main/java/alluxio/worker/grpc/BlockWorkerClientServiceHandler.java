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

package alluxio.worker.grpc;

import alluxio.RpcUtils;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.AsyncCacheResponse;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.CacheResponse;
import alluxio.grpc.ClearMetricsRequest;
import alluxio.grpc.ClearMetricsResponse;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.MoveBlockRequest;
import alluxio.grpc.MoveBlockResponse;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.TaskStatus;
import alluxio.grpc.WriteRequestMarshaller;
import alluxio.grpc.WriteResponse;
import alluxio.grpc.FreeWorkerRequest;
import alluxio.grpc.FreeWorkerResponse;
import alluxio.grpc.DecommissionWorkerRequest;
import alluxio.grpc.DecommissionWorkerResponse;
import alluxio.grpc.HandleRPCRequest;
import alluxio.grpc.HandleRPCResponse;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.underfs.UfsManager;
import alluxio.util.IdUtils;
import alluxio.util.SecurityUtils;
import alluxio.worker.AlluxioWorkerProcess;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.AllocateOptions;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.DefaultBlockWorker;

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
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
  private static final AtomicBoolean mReadOnlyMode = new AtomicBoolean(false);
  private static final AtomicInteger mReadRequestCount = new AtomicInteger(0);
  private static final AtomicInteger mWriteRequestCount = new AtomicInteger(0);
  private final WorkerProcess mWorkerProcess;
  private final Lock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();

  /**
   * Creates a new implementation of gRPC BlockWorker interface.
   *
   * @param workerProcess the worker process
   * @param domainSocketEnabled is using domain sockets
   */
  public BlockWorkerClientServiceHandler(WorkerProcess workerProcess,
      boolean domainSocketEnabled) {
    mBlockWorker = (DefaultBlockWorker) workerProcess.getWorker(BlockWorker.class);
    mUfsManager = workerProcess.getUfsManager();
    mDomainSocketEnabled = domainSocketEnabled;
    mWorkerProcess = workerProcess;
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
  public StreamObserver<alluxio.grpc.WriteRequest> writeBlock(
      StreamObserver<WriteResponse> responseObserver) {
    // TODO(Tony Sun): Consider an elegant return method.
    if (mReadOnlyMode.get())
      return null;
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
    // TODO(Tony Sun): Consider an elegant return method.
    if (mReadOnlyMode.get())
      return null;
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
    // TODO(Tony Sun): Consider an elegant return method.
    if (mReadOnlyMode.get())
      return;
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

  public boolean setWorkerToBeReadOnlyMode() {
    return mReadOnlyMode.compareAndSet(false, true);
  }

  // TODO(Tony Sun): Not finished, This method works for set ROM, waiting for the reading and writing to be ended.
  // Refactoring: Extract the persistence function.
  @Override
  public void decommissionWorker(DecommissionWorkerRequest request,
     StreamObserver<DecommissionWorkerResponse> responseObserver) {
    // If target worker is not in ROM now, return and do nothing.
    if (!mReadOnlyMode.get())
      return;
    RpcUtils.call(LOG, () -> {
      System.out.println("Now there are no reading and writing grpc. Next step is committing persist jobs");
      return DecommissionWorkerResponse.getDefaultInstance();
    }, "decommissionWorker", "request=%s", responseObserver, request);
  }

  public void handleRPC(HandleRPCRequest request, StreamObserver<HandleRPCResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      // 1. Set ROM
      if (setWorkerToBeReadOnlyMode()) {
        System.out.println("Setting ROM.");
        // 2. Reboot Grpc Server
        ((AlluxioWorkerProcess)mWorkerProcess).rebootDataServer();
      }
      else {
        System.out.println("ROM has been set.");
      }
      // TODO(Tony Sun): In the future, add logic to monitor the running read and write service, add timeout.
      System.out.println("The return below will raise Exception, " +
              "because the older data server has been shut down.");
      return HandleRPCResponse.getDefaultInstance();
    }, "handleRPC", "request=%s", responseObserver, request);
  }

  @Override
  public void freeWorker(FreeWorkerRequest request, StreamObserver<FreeWorkerResponse> responseObserver) {
    long sessionId = IdUtils.createSessionId();
    RpcUtils.call(LOG, () -> {
      mBlockWorker.freeCurrentWorker();
      return FreeWorkerResponse.getDefaultInstance();
    }, "freeWorker", "request=%s", responseObserver, request);
  }

  public boolean getReadOnlyModeStatus() {
    return mReadOnlyMode.get();
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
