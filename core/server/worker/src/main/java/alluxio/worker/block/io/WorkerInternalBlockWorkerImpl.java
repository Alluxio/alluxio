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

package alluxio.worker.block.io;

import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.wire.BlockReadRequest;
import alluxio.worker.block.BlockWorker;

import java.io.IOException;

/**
 * This class enables worker internal applications to call the worker operations directly
 * without going through the external RPC frameworks.
 */
public class WorkerInternalBlockWorkerImpl implements WorkerInternalBlockWorker {
  private final BlockWorker mBlockWorker;

  /**
   * Constructs a {@link WorkerInternalBlockWorkerImpl}.
   *
   * @param blockWorker the block worker to trigger worker operations from
   */
  public WorkerInternalBlockWorkerImpl(BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
  }

  @Override
  public void asyncCache(AsyncCacheRequest request) {
    mBlockWorker.submitAsyncCacheRequest(request);
  }

  @Override
  public BlockReader getBlockReader(BlockReadRequest request) throws IOException,
      BlockDoesNotExistException, InvalidWorkerStateException,
      BlockAlreadyExistsException, WorkerOutOfSpaceException {
    return mBlockWorker.getBlockReader(request);
  }

  @Override
  public void cleanBlockReader(BlockReader reader, BlockReadRequest request)
      throws IOException, BlockAlreadyExistsException, WorkerOutOfSpaceException {
    mBlockWorker.cleanBlockReader(reader, request);
  }
}
