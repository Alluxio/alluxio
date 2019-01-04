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
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.util.IdUtils;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * gRPC handler that handles short circuit read requests.
 */
@NotThreadSafe
class ShortCircuitBlockReadHandler implements StreamObserver<OpenLocalBlockRequest> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ShortCircuitBlockReadHandler.class);

  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();
  /** The block worker. */
  private final BlockWorker mWorker;
  private final StreamObserver<OpenLocalBlockResponse> mResponseObserver;
  private OpenLocalBlockRequest mRequest;
  /** The lock Id of the block being read. */
  private long mLockId;
  private long mSessionId;

  /**
   * Creates an instance of {@link ShortCircuitBlockReadHandler}.
   *
   * @param blockWorker the block worker
   */
  ShortCircuitBlockReadHandler(BlockWorker blockWorker,
      StreamObserver<OpenLocalBlockResponse> responseObserver) {
    mWorker = blockWorker;
    mLockId = BlockLockManager.INVALID_LOCK_ID;
    mResponseObserver = responseObserver;
  }

  /**
   * Handles block open request.
   */
  @Override
  public void onNext(OpenLocalBlockRequest request) {
    RpcUtils.nettyRPCAndLog(LOG, new RpcUtils.NettyRpcCallable<OpenLocalBlockResponse>() {
      @Override
      public OpenLocalBlockResponse call() throws Exception {
        Preconditions.checkState(mRequest == null);
        mRequest = request;
        if (mLockId == BlockLockManager.INVALID_LOCK_ID) {
          mSessionId = IdUtils.createSessionId();
          // TODO(calvin): Update the locking logic so this can be done better
          if (mRequest.getPromote()) {
            try {
              mWorker
                  .moveBlock(mSessionId, mRequest.getBlockId(), mStorageTierAssoc.getAlias(0));
            } catch (BlockDoesNotExistException e) {
              LOG.debug("Block {} to promote does not exist in Alluxio: {}",
                  mRequest.getBlockId(), e.getMessage());
            } catch (Exception e) {
              LOG.warn("Failed to promote block {}: {}", mRequest.getBlockId(), e.getMessage());
            }
          }
          mLockId = mWorker.lockBlock(mSessionId, mRequest.getBlockId());
          mWorker.accessBlock(mSessionId, mRequest.getBlockId());
        } else {
          LOG.warn("Lock block {} without releasing previous block lock {}.",
              mRequest.getBlockId(), mLockId);
          throw new InvalidWorkerStateException(
              ExceptionMessage.LOCK_NOT_RELEASED.getMessage(mLockId));
        }
        OpenLocalBlockResponse response = OpenLocalBlockResponse.newBuilder()
            .setPath(mWorker.readBlock(mSessionId, mRequest.getBlockId(), mLockId)).build();
        return response;
      }

      @Override
      public void exceptionCaught(Throwable e) {
        if (mLockId != BlockLockManager.INVALID_LOCK_ID) {
          try {
            mWorker.unlockBlock(mLockId);
          } catch (BlockDoesNotExistException ee) {
            LOG.error("Failed to unlock block {}.", mRequest.getBlockId(), e);
          }
          mLockId = BlockLockManager.INVALID_LOCK_ID;
        }
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(e));
      }
    }, "OpenBlock", true, false, "Session=%d, Request=%s",
        mResponseObserver, mSessionId, mRequest);
  }

  @Override
  public void onError(Throwable t) {
    if (mLockId != BlockLockManager.INVALID_LOCK_ID) {
      try {
        mWorker.unlockBlock(mLockId);
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed to unlock lock {} with error {}.", mLockId, e.getMessage());
      }
      mWorker.cleanupSession(mSessionId);
    }
    mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(t));
  }

  /**
   * Handles block close request. No exceptions should be thrown.
   */
  @Override
  public void onCompleted() {
    RpcUtils.nettyRPCAndLog(LOG, new RpcUtils.NettyRpcCallable<OpenLocalBlockResponse>() {
      @Override
      public OpenLocalBlockResponse call() throws Exception {
        if (mLockId != BlockLockManager.INVALID_LOCK_ID) {
          mWorker.unlockBlock(mLockId);
          mLockId = BlockLockManager.INVALID_LOCK_ID;
        } else if (mRequest != null) {
          LOG.warn("Close a closed block {}.", mRequest.getBlockId());
        }
        return null;
      }

      @Override
      public void exceptionCaught(Throwable e) {
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(e));
        mLockId = BlockLockManager.INVALID_LOCK_ID;
      }
    }, "CloseBlock", false, true, "Session=%d, Request=%s",
        mResponseObserver, mSessionId, mRequest);
  }
}
