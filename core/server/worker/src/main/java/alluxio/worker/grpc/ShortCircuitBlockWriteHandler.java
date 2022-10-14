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
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.util.IdUtils;
import alluxio.util.LogUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.CreateBlockOptions;

import com.google.common.base.Preconditions;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A handler that handles short circuit read requests.
 */
@NotThreadSafe
class ShortCircuitBlockWriteHandler implements StreamObserver<CreateLocalBlockRequest> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ShortCircuitBlockWriteHandler.class);

  private static final long INVALID_SESSION_ID = -1;

  /** The block worker. */
  private final BlockWorker mBlockWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StreamObserver<CreateLocalBlockResponse> mResponseObserver;
  private CreateLocalBlockRequest mRequest = null;
  private long mSessionId = INVALID_SESSION_ID;

  /**
   * Creates an instance of {@link ShortCircuitBlockWriteHandler}.
   *
   * @param blockWorker the block worker
   */
  ShortCircuitBlockWriteHandler(BlockWorker blockWorker,
      StreamObserver<CreateLocalBlockResponse> responseObserver) {
    mBlockWorker = blockWorker;
    mResponseObserver = responseObserver;
  }

  /**
   * Handles request to create local block. No exceptions should be thrown.
   *
   * @param request a create request
   */
  @Override
  public void onNext(CreateLocalBlockRequest request) {
    final String methodName = request.getOnlyReserveSpace() ? "ReserveSpace" : "CreateBlock";
    RpcUtils.streamingRPCAndLog(LOG, new RpcUtils.StreamingRpcCallable<CreateLocalBlockResponse>() {
      @Override
      public CreateLocalBlockResponse call() throws Exception {
        if (request.getOnlyReserveSpace()) {
          mBlockWorker.requestSpace(mSessionId, request.getBlockId(), request.getSpaceToReserve());
          return CreateLocalBlockResponse.newBuilder().build();
        } else {
          Preconditions.checkState(mRequest == null);
          mRequest = request;
          Preconditions.checkState(mSessionId == INVALID_SESSION_ID,
              MessageFormat.format("session {0,number,#} is not closed.", mSessionId));
          mSessionId = IdUtils.createSessionId();
          String path =
              mBlockWorker.createBlock(mSessionId, request.getBlockId(), request.getTier(),
                  new CreateBlockOptions(null, request.getMediumType(),
                      request.getSpaceToReserve()));
          return CreateLocalBlockResponse.newBuilder().setPath(path).build();
        }
      }

      @Override
      public void exceptionCaught(Throwable throwable) {
        if (mSessionId != INVALID_SESSION_ID) {
          // In case the client is a UfsFallbackDataWriter, DO NOT clean the temp blocks.
          if (throwable instanceof ResourceExhaustedRuntimeException
              && request.hasCleanupOnFailure() && !request.getCleanupOnFailure()) {
            mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(throwable));
            return;
          }
          mBlockWorker.cleanupSession(mSessionId);
          mSessionId = INVALID_SESSION_ID;
        }
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(throwable));
      }
    }, methodName, true, false, mResponseObserver, "Session=%d, Request=%s", mSessionId, request);
  }

  @Override
  public void onCompleted() {
    handleBlockCompleteRequest(false);
  }

  /**
   * Handles cancel event from the client.
   */
  public void onCancel() {
    handleBlockCompleteRequest(true);
  }

  @Override
  public void onError(Throwable t) {
    LogUtils.warnWithException(LOG, "Exception occurred processing write request {}.", mRequest, t);
    if (t instanceof StatusRuntimeException
        && ((StatusRuntimeException) t).getStatus().getCode() == Status.Code.CANCELLED) {
      // Cancellation is already handled.
      return;
    }
    // The RPC handlers do not throw exceptions. All the exception seen here is either
    // network exception or some runtime exception (e.g. NullPointerException).
    LOG.error("Failed to handle RPCs.", t);
    if (mSessionId != INVALID_SESSION_ID) {
      mBlockWorker.cleanupSession(mSessionId);
      mSessionId = INVALID_SESSION_ID;
    }
    mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(t));
  }

  /**
   * Handles complete block request. No exceptions should be thrown.
   *
   */
  public void handleBlockCompleteRequest(boolean isCanceled) {
    final String methodName = isCanceled ? "AbortBlock" : "CommitBlock";
    RpcUtils.streamingRPCAndLog(LOG, new RpcUtils.StreamingRpcCallable<CreateLocalBlockResponse>() {
      @Override
      public CreateLocalBlockResponse call() throws Exception {
        if (mRequest == null) {
          return null;
        }
        Context newContext = Context.current().fork();
        Context previousContext = newContext.attach();
        try {
          if (isCanceled) {
            mBlockWorker.abortBlock(mSessionId, mRequest.getBlockId());
          } else {
            mBlockWorker.commitBlock(mSessionId, mRequest.getBlockId(), mRequest.getPinOnCreate());
          }
        } finally {
          newContext.detach(previousContext);
        }
        mSessionId = INVALID_SESSION_ID;
        return null;
      }

      @Override
      public void exceptionCaught(Throwable throwable) {
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(throwable));
        mSessionId = INVALID_SESSION_ID;
      }
    }, methodName, false, !isCanceled, mResponseObserver, "Session=%d, Request=%s", mSessionId,
        mRequest);
  }
}
