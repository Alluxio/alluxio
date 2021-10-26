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

package alluxio.master.block;

import alluxio.RpcUtils;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class handles the master side logic of the register stream.
 * The stream lifecycle is internal to the master.
 * In other words, there should be no external control on the request/response
 * observers external to this class.
 */
public class RegisterStreamObserver implements StreamObserver<RegisterWorkerPRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterStreamObserver.class);

  private WorkerRegisterContext mContext;
  private final BlockMaster mBlockMaster;
  private final io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerPResponse> mResponseObserver;
  // Records the error from the worker side, if any
  private AtomicReference<Throwable> mErrorReceived = new AtomicReference<>();

  RegisterStreamObserver(BlockMaster blockMaster, io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerPResponse> responseObserver) {
    mBlockMaster = blockMaster;
    mResponseObserver = responseObserver;
  }

  @Override
  public void onNext(RegisterWorkerPRequest chunk) {
    System.out.println("handled by stream observer " + this);
    final long workerId = chunk.getWorkerId();
    final boolean isHead = isFirstMessage(chunk);
    LOG.info("{} - Register worker request is {} bytes, containing {} LocationBlockIdListEntry. Worker {}, isHead {}",
            Thread.currentThread().getId(),
            chunk.getSerializedSize(),
            chunk.getCurrentBlocksCount(),
            workerId,
            isHead);

    StreamObserver<RegisterWorkerPRequest> requestObserver = this;
    String methodName = isHead ? "registerWorkerStart" : "registerWorkerStream";

    RpcUtils.streamingRPCAndLog(LOG, new RpcUtils.StreamingRpcCallable<RegisterWorkerPResponse>() {
      @Override
      public RegisterWorkerPResponse call() throws Exception {
        // If an error was received earlier, the stream is no longer open
        Preconditions.checkState(mErrorReceived.get() == null,
            "The stream has been closed due to an earlier error received: %s", mErrorReceived.get());

        // Initialize the context on the 1st message
        synchronized (requestObserver) {
          if (mContext == null) {
            LOG.debug("Initializing the WorkerRegisterContext on the 1st request");
            Preconditions.checkState(isHead, "WorkerRegisterContext is not initialized but the request is not the 1st in a stream");

            LOG.debug("Initializing context for {}", workerId);
            mContext = WorkerRegisterContext.create(mBlockMaster, workerId, requestObserver, mResponseObserver);
            LOG.debug("Context created for {}", workerId);
          }
        }

        // Verify the context is successfully initialized
        Preconditions.checkState(mContext != null, "Stream message received from the client side but the context is not initialized");
        Preconditions.checkState(mContext.isOpen(), "Context is not open");

        if (isHead) {
          mBlockMaster.workerRegisterStart(mContext, chunk);
          System.out.println("WorkerRegisterStart finished, update TS");
        } else {
          mBlockMaster.workerRegisterBatch(mContext, chunk);
          System.out.println("WorkerRegisterBatch finished, update TS");
        }
        mContext.updateTs();
        // Return an ACK to the worker so it sends the next batch
        return RegisterWorkerPResponse.newBuilder().build();
      }

      @Override
      public void exceptionCaught(Throwable e) {
        // When an exception occurs on the master side, close the context and
        // propagate the exception to the worker side.
        cleanup();
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(e));
      }
    }, methodName, true, false, mResponseObserver, "Request=%s", chunk);
  }

  @Override
  // This means the server side has received an error from the worker side, close the context.
  // When an error occurs on the worker side so that it cannot proceed with the register logic,
  // the worker will send the error to the master and close itself.
  // The master will then receive the error, abort the stream and close itself.
  public void onError(Throwable t) {
    System.out.println("Master received exception " + t);
    if (t instanceof TimeoutException) {
      System.out.println("Timeout signal received from the WorkerRegisterStreamGCExecutor. "
          + "Closing context for hanging worker.");
      cleanup();
      mResponseObserver.onError(AlluxioStatusException.fromThrowable(t).toGrpcStatusException());
      return;
    }
    // Otherwise the exception is from the worker
    System.out.println("handled by stream observer " + this);
    mErrorReceived.set(t);
    System.out.println("Received error from worker: " + t);
    LOG.error("Received error from the worker side during the streaming register call: {}", t.getMessage());
    cleanup();
  }

  @Override
  public void onCompleted() {
    LOG.info("{} - Register stream completed on the client side", Thread.currentThread().getId());

    String methodName = "registerWorkerComplete";
    RpcUtils.streamingRPCAndLog(LOG, new RpcUtils.StreamingRpcCallable<RegisterWorkerPResponse>() {
      @Override
      public RegisterWorkerPResponse call() throws Exception {
        Preconditions.checkState(mErrorReceived.get() == null,
            "The stream has been closed due to an earlier error received: %s", mErrorReceived.get());
        Preconditions.checkState(mContext != null,
            "Complete message received from the client side but the context is not initialized");
        Preconditions.checkState(mContext.isOpen(), "Context is not open");

        mBlockMaster.workerRegisterFinish(mContext);
        System.out.println("WorkerRegisterFinish finished, update TS");
        mContext.updateTs();

        cleanup();
        // No response because sendResponse=false
        return null;
      }

      @Override
      public void exceptionCaught(Throwable e) {
        // When an exception occurs on the master side, close the context and
        // propagate the exception to the worker side.
        cleanup();
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(e));
      }
    }, methodName, false, true, mResponseObserver, "WorkerId=%s",
            mContext == null ? "NONE" : mContext.getWorkerId());
  }

  private boolean isFirstMessage(alluxio.grpc.RegisterWorkerPRequest chunk) {
    return chunk.getStorageTiersCount() > 0;
  }

  private void cleanup() {
    synchronized (this) {
      if (mContext == null) {
        LOG.debug("The stream is closed before the context is initialized. Nothing to clean up.");
        return;
      }
      LOG.debug("Unlocking worker {}", mContext.getWorkerId());
      mContext.close();
      LOG.debug("Context closed");

      Preconditions.checkState(!mContext.isOpen(),
          "Failed to properly close the WorkerRegisterContext!");
    }
  }
}
