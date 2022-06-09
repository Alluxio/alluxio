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

import static alluxio.worker.block.BlockMetadataManager.WORKER_STORAGE_TIER_ASSOC;
import static com.google.common.base.Preconditions.checkState;

import alluxio.RpcUtils;
import alluxio.exception.BlockDoesNotExistRuntimeException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.util.IdUtils;
import alluxio.util.LogUtils;
import alluxio.worker.block.AllocateOptions;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.DefaultBlockWorker;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TieredBlockMeta;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * gRPC handler that handles short circuit read requests.
 */
@NotThreadSafe
class ShortCircuitBlockReadHandler implements StreamObserver<OpenLocalBlockRequest> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ShortCircuitBlockReadHandler.class);

  private final TieredBlockStore mLocalBlockStore;
  private final StreamObserver<OpenLocalBlockResponse> mResponseObserver;
  private OpenLocalBlockRequest mRequest;
  /** The lock id of the block being read. */
  private OptionalLong mLockId;
  private long mSessionId;

  /**
   * Creates an instance of {@link ShortCircuitBlockReadHandler}.
   *
   * @param localBlockStore the local block store
   */
  ShortCircuitBlockReadHandler(TieredBlockStore localBlockStore,
                               StreamObserver<OpenLocalBlockResponse> responseObserver) {
    mLocalBlockStore = localBlockStore;
    mLockId = OptionalLong.empty();
    mResponseObserver = responseObserver;
  }

  /**
   * Handles block open request.
   */
  @Override
  public void onNext(OpenLocalBlockRequest request) {
    RpcUtils.streamingRPCAndLog(LOG, new RpcUtils.StreamingRpcCallable<OpenLocalBlockResponse>() {
      @Override
      public OpenLocalBlockResponse call() throws Exception {
        checkState(mRequest == null);
        mRequest = request;
        if (mLockId.isPresent()) {
          LOG.warn("Lock block {} without releasing previous block lock {}.",
              mRequest.getBlockId(), mLockId);
          throw new InvalidWorkerStateException(
              MessageFormat.format("session {0,number,#} is not closed.", mLockId));
        }
        mSessionId = IdUtils.createSessionId();
        // TODO(calvin): Update the locking logic so this can be done better
        Optional<? extends BlockMeta> meta =
            mLocalBlockStore.getVolatileBlockMeta(mRequest.getBlockId());
        if (!meta.isPresent()) {
          throw new BlockDoesNotExistRuntimeException(mRequest.getBlockId());
        }
        if (mRequest.getPromote() && meta.get() instanceof TieredBlockMeta) {
          // TODO(calvin): Move this logic into BlockStore#moveBlockInternal if possible
          // Because the move operation is expensive, we first check if the operation is necessary
          BlockStoreLocation dst = BlockStoreLocation.anyDirInTier(
              WORKER_STORAGE_TIER_ASSOC.getAlias(0));
          TieredBlockMeta blockMeta = (TieredBlockMeta) (meta.get());
          if (!blockMeta.getBlockLocation().belongsTo(dst)) {
            // Execute the block move if necessary
            mLocalBlockStore.moveBlock(mSessionId, mRequest.getBlockId(),
                AllocateOptions.forMove(dst));
          }
        }
        mLockId = mLocalBlockStore.pinBlock(mSessionId, mRequest.getBlockId());
        mLocalBlockStore.accessBlock(mSessionId, mRequest.getBlockId());
        DefaultBlockWorker.Metrics.WORKER_ACTIVE_CLIENTS.inc();
        return OpenLocalBlockResponse.newBuilder()
            .setPath(meta.get().getPath())
            .build();
      }

      @Override
      public void exceptionCaught(Throwable e) {
        if (mLockId.isPresent()) {
          DefaultBlockWorker.Metrics.WORKER_ACTIVE_CLIENTS.dec();
          mLocalBlockStore.unpinBlock(mLockId.getAsLong());
          mLockId = OptionalLong.empty();
        }
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(e));
      }
    }, "OpenBlock", true, false, mResponseObserver, "Session=%d, Request=%s",
        mSessionId, request);
  // Must use request object directly for this log as mRequest is only set in the Callable
  }

  @Override
  public void onError(Throwable t) {
    LogUtils.warnWithException(LOG, "Exception occurred processing read request {}.", mRequest, t);
    if (mLockId.isPresent()) {
      DefaultBlockWorker.Metrics.WORKER_ACTIVE_CLIENTS.dec();
      mLocalBlockStore.unpinBlock(mLockId.getAsLong());
      mLocalBlockStore.cleanupSession(mSessionId);
    }
    mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(t));
  }

  /**
   * Handles block close request. No exceptions should be thrown.
   */
  @Override
  public void onCompleted() {
    RpcUtils.streamingRPCAndLog(LOG, new RpcUtils.StreamingRpcCallable<OpenLocalBlockResponse>() {
      @Override
      public OpenLocalBlockResponse call() {
        if (mLockId.isPresent()) {
          DefaultBlockWorker.Metrics.WORKER_ACTIVE_CLIENTS.dec();
          mLocalBlockStore.unpinBlock(mLockId.getAsLong());
          mLockId = OptionalLong.empty();
        } else if (mRequest != null) {
          LOG.warn("Close a closed block {}.", mRequest.getBlockId());
        }
        return null;
      }

      @Override
      public void exceptionCaught(Throwable e) {
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(e));
        mLockId = OptionalLong.empty();
      }
    }, "CloseBlock", false, true, mResponseObserver, "Session=%d, Request=%s",
        mSessionId, mRequest);
  }
}
