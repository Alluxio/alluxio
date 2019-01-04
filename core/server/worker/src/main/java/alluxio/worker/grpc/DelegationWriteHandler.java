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

import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockWorker;

import io.grpc.stub.StreamObserver;

/**
 * A write request handler that delegate worker write request of different types
 * to corresponding write handlers.
 */
public class DelegationWriteHandler implements StreamObserver<alluxio.grpc.WriteRequest> {
  private final StreamObserver<WriteResponse> mResponseObserver;
  private final WorkerProcess mWorkerProcess;
  private AbstractWriteHandler mWriteHandler;

  /**
   * @param workerProcess the worker process instance
   * @param responseObserver the response observer of the gRPC stream
   */
  public DelegationWriteHandler(WorkerProcess workerProcess,
      StreamObserver<WriteResponse> responseObserver) {
    mWorkerProcess = workerProcess;
    mResponseObserver = responseObserver;
  }

  private AbstractWriteHandler createWriterHandler(alluxio.grpc.WriteRequest request) {
    switch (request.getCommand().getType()) {
      case ALLUXIO_BLOCK:
        return new BlockWriteHandler(mWorkerProcess.getWorker(BlockWorker.class),
            mResponseObserver);
      case UFS_FILE:
        return new UfsFileWriteHandler(mWorkerProcess.getUfsManager(),
            mResponseObserver);
      case UFS_FALLBACK_BLOCK:
        return new UfsFallbackBlockWriteHandler(
            mWorkerProcess.getWorker(BlockWorker.class), mWorkerProcess.getUfsManager(),
            mResponseObserver);
      default:
        throw new IllegalArgumentException(String.format("Invalid request type %s",
            request.getCommand().getType().name()));
    }
  }

  @Override
  public void onNext(WriteRequest request) {
    if (mWriteHandler == null) {
      mWriteHandler = createWriterHandler(request);
    }
    mWriteHandler.write(request);
  }

  @Override
  public void onError(Throwable t) {
    if (mWriteHandler != null) {
      mWriteHandler.onError(t);
    }
  }

  @Override
  public void onCompleted() {
    if (mWriteHandler != null) {
      mWriteHandler.onCompleted();
    }
  }

  /**
   * Handles cancel event from the client.
   */
  public void onCancel() {
    if (mWriteHandler != null) {
      mWriteHandler.onCancel();
    }
  }
}
