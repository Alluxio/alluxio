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
import alluxio.exception.status.CancelledException;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

public class RegisterStreamObserver implements StreamObserver<RegisterWorkerPRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterStreamObserver.class);

  private WorkerRegisterContext mContext;

  final BlockMaster mBlockMaster;
  final io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerPResponse> mResponseObserver;
  private AtomicReference<Throwable> mErrorReceived = new AtomicReference<>();

  RegisterStreamObserver(BlockMaster blockMaster, io.grpc.stub.StreamObserver<alluxio.grpc.RegisterWorkerPResponse> responseObserver) {
    mBlockMaster = blockMaster;
    mResponseObserver = responseObserver;
  }

  boolean isFirstMessage(alluxio.grpc.RegisterWorkerPRequest chunk) {
    return chunk.getStorageTiersCount() > 0;
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
        // Initialize the context on the 1st message
        synchronized (requestObserver) {
          if (mContext == null) {
            if (mErrorReceived.get() != null) {
              // An error has been received before the context is initialized
              CancelledException e = new CancelledException("Received error from worker before the stream was opened");
              e.addSuppressed(mErrorReceived.get());
              throw e;
            }
            LOG.debug("Initializing the WorkerRegisterContext on the 1st request");
            Preconditions.checkState(isHead, "WorkerRegisterContext is not initialized but the request is not the 1st in a stream");

            LOG.debug("Initializing context for {}", workerId);
            mContext = WorkerRegisterContext.create(mBlockMaster, workerId, requestObserver, mResponseObserver);
            LOG.debug("Context created for {}", workerId);
          }
        }

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
      // TODO(jiacheng): test this
      public void exceptionCaught(Throwable e) {
        // Record the error received from worker
        mErrorReceived.set(e);
        // When an exception occurs on the master side, close the context and
        // propagate the exception to the worker side.
        cleanup();
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(e));
      }
    }, methodName, true, false, mResponseObserver, "Worker=%s", workerId);
  }

  @Override
  // This means the server side has received an error from the worker side, close the context.
  // When an error occurs on the worker side so that it cannot proceed with the register logic,
  // the worker will send the error to the master and close itself.
  // The master will then receive the error, abort the stream and close itself.
  public void onError(Throwable t) {
    System.out.println("handled by stream observer " + this);
    mErrorReceived.set(t);
    System.out.println("Received error from worker: " + t);
    // TODO(jiacheng): Do not log the full exception, the full stacktrace should be found
    //  on the worker and the master log should be clean with only a warning message
    LOG.error("Received error from the worker side during the streaming register call", t);
    cleanup();
  }

  @Override
  public void onCompleted() {
    System.out.println("handled by stream observer " + this);

    LOG.info("{} - Register stream completed on the client side", Thread.currentThread().getId());

    String methodName = "registerWorkerComplete";
    RpcUtils.streamingRPCAndLog(LOG, new RpcUtils.StreamingRpcCallable<RegisterWorkerPResponse>() {
      @Override
      public RegisterWorkerPResponse call() throws Exception {
        // TODO(jiacheng): if mError, then propagate the information
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
      // TODO(jiacheng): test this
      public void exceptionCaught(Throwable e) {
        // When an exception occurs on the master side, close the context and
        // propagate the exception to the worker side.
        cleanup();
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(e));
      }
      // TODO(jiacheng): log more information
    }, methodName, false, true, mResponseObserver, "WorkerId=%s",
            mContext == null ? "NONE" : mContext.getWorkerId());
  }

  void cleanup() {
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
