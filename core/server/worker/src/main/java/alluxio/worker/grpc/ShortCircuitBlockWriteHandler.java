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

import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.util.IdUtils;
import alluxio.util.RpcUtilsNew;
import alluxio.worker.block.BlockWorker;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Netty handler that handles short circuit read requests.
 */
@NotThreadSafe
class ShortCircuitBlockWriteHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(ShortCircuitBlockWriteHandler.class);

  private static final long INVALID_SESSION_ID = -1;

  /** The block worker. */
  private final BlockWorker mBlockWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();
  private final CreateLocalBlockRequest mRequest;
  private final StreamObserver<CreateLocalBlockResponse> mResponseObserver;

  private long mSessionId = INVALID_SESSION_ID;

  /**
   * Creates an instance of {@link ShortCircuitBlockWriteHandler}.
   *
   * @param blockWorker the block worker
   */
  ShortCircuitBlockWriteHandler(BlockWorker blockWorker,
      CreateLocalBlockRequest request,
      StreamObserver<CreateLocalBlockResponse> responseObserver) {
    mBlockWorker = blockWorker;
    mRequest = request;
    mResponseObserver = responseObserver;
  }

  public void exceptionCaught(Throwable throwable) {
    // The RPC handlers do not throw exceptions. All the exception seen here is either
    // network exception or some runtime exception (e.g. NullPointerException).
    LOG.error("Failed to handle RPCs.", throwable);
    if (mSessionId != INVALID_SESSION_ID) {
      mBlockWorker.cleanupSession(mSessionId);
      mSessionId = INVALID_SESSION_ID;
    }
    mResponseObserver.onError(throwable);
  }

  /**
   * Handles request to create local block. No exceptions should be
   * thrown.
   */
  public  void handleBlockCreateRequest() {
    final String methodName = mRequest.getOnlyReserveSpace() ? "ReserveSpace" : "CreateBlock";
    RpcUtilsNew.nettyRPCAndLog(LOG, new RpcUtilsNew.NettyRpcCallable<CreateLocalBlockResponse>() {
      @Override
      public CreateLocalBlockResponse call() throws Exception {
        if (mRequest.getOnlyReserveSpace()) {
          mBlockWorker
              .requestSpace(mSessionId, mRequest.getBlockId(), mRequest.getSpaceToReserve());
          return CreateLocalBlockResponse.newBuilder().build();
        } else {
          if (mSessionId == INVALID_SESSION_ID) {
            mSessionId = IdUtils.createSessionId();
            String path = mBlockWorker.createBlock(mSessionId, mRequest.getBlockId(),
                mStorageTierAssoc.getAlias(mRequest.getTier()), mRequest.getSpaceToReserve());
            CreateLocalBlockResponse response =
                CreateLocalBlockResponse.newBuilder().setPath(path).build();
            return response;
          } else {
            LOG.warn("Create block {} without closing the previous session {}.",
                mRequest.getBlockId(), mSessionId);
            throw new InvalidWorkerStateException(
                ExceptionMessage.SESSION_NOT_CLOSED.getMessage(mSessionId));
          }
        }
      }

      @Override
      public void exceptionCaught(Throwable throwable) {
        if (mSessionId != INVALID_SESSION_ID) {
          // In case the client is a UfsFallbackPacketWriter, DO NOT clean the temp blocks.
          if (throwable instanceof alluxio.exception.WorkerOutOfSpaceException
              && mRequest.hasCleanupOnFailure() && !mRequest.getCleanupOnFailure()) {
            mResponseObserver.onError(AlluxioStatusException.fromThrowable(throwable));
            return;
          }
          mBlockWorker.cleanupSession(mSessionId);
          mSessionId = INVALID_SESSION_ID;
        }
        mResponseObserver.onError(AlluxioStatusException.fromThrowable(throwable));
      }
    }, methodName, true, false, "Session=%d, Request=%s",
        mResponseObserver, mSessionId, mRequest);
  }

  /**
   * Handles complete block request. No exceptions should be thrown.
   *
   */
  public void handleBlockCompleteRequest(boolean isCanceled) {
    final String methodName = isCanceled ? "AbortBlock" : "CommitBlock";
    RpcUtilsNew.nettyRPCAndLog(LOG, new RpcUtilsNew.NettyRpcCallable<CreateLocalBlockResponse>() {
        @Override
        public CreateLocalBlockResponse call() throws Exception {
          Context newContext = Context.current().fork();
          Context previousContext = newContext.attach();
          try {
            if (isCanceled) {
              mBlockWorker.abortBlock(mSessionId, mRequest.getBlockId());
            } else {
              mBlockWorker.commitBlock(mSessionId, mRequest.getBlockId());
            }
          } finally {
            newContext.detach(previousContext);
          }
          mSessionId = INVALID_SESSION_ID;
          return null;
        }

        @Override
        public void exceptionCaught(Throwable throwable) {
          mResponseObserver.onError(AlluxioStatusException.fromThrowable(throwable));
          mSessionId = INVALID_SESSION_ID;
        }
      }, methodName, false, true, "Session=%d, Request=%s",
        mResponseObserver, mSessionId, mRequest);
  }
}
