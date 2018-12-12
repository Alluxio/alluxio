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

import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.worker.WorkerProcess;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server side implementation of the gRPC BlockWorker interface.
 */
public class BlockWorkerImpl extends BlockWorkerGrpc.BlockWorkerImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerImpl.class);
  private WorkerProcess mWorkerProcess;

  /**
   * Creates a new implementation of gRPC BlockWorker interface.
   *
   * @param workerProcess the worker process
   */
  public BlockWorkerImpl(WorkerProcess workerProcess) {
    mWorkerProcess = workerProcess;
  }

  @Override
  public void readBlock(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
    // TODO(feng): Implements readBlock
  }

  @Override
  public StreamObserver<alluxio.grpc.WriteRequest> writeBlock(
      final StreamObserver<WriteResponse> responseObserver) {
    // TODO(feng): Implements writeBlock
    return new StreamObserver<alluxio.grpc.WriteRequest>() {
      @Override
      public void onNext(WriteRequest request) {
      }

      @Override
      public void onError(Throwable t) {
      }

      @Override
      public void onCompleted() {
      }
    };
  }
}
