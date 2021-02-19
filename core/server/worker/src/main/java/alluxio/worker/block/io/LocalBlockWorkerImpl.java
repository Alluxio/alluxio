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

import alluxio.Sessions;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.grpc.BlockReadRequest;

import java.io.IOException;

/**
 * This class enables worker internal applications to call the worker operations directly
 * without going through the external RPC frameworks.
 */
public class LocalBlockWorkerImpl implements LocalBlockWorker {
  private final BlockWorker mBlockWorker;

  /**
   * Constructs a {@link LocalBlockWorkerImpl}.
   *
   * @param blockWorker the block worker
   */
  public LocalBlockWorkerImpl(BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
  }

  @Override
  public void asyncCache(AsyncCacheRequest request) {
    // TODO(lu) trigger asyncCache after https://github.com/Alluxio/alluxio/pull/12864 merged
  }

  @Override
  public void moveBlock(long blockId, String mediumType) throws BlockDoesNotExistException,
      BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    mBlockWorker.moveBlock(Sessions.FUSE_SESSION_ID, blockId, mediumType);
  }

  @Override
  public void removeBlock(long blockId) throws alluxio.exception.InvalidWorkerStateException,
      alluxio.exception.BlockDoesNotExistException, java.io.IOException {
    // TODO(lu) exception handling
    mBlockWorker.removeBlock(Sessions.FUSE_SESSION_ID, blockId);
  }

  @Override
  public BlockReader getBlockReader(BlockReadRequest request) throws IOException,
      BlockDoesNotExistException, InvalidWorkerStateException,
      BlockAlreadyExistsException, WorkerOutOfSpaceException {
    // TODO(lu) modify after https://github.com/Alluxio/alluxio/pull/12838
  }

  @Override
  public void cleanBlockReader(long sessionId, long blockId, BlockReader reader)
      throws IOException, BlockAlreadyExistsException, WorkerOutOfSpaceException {
    // TODO(lu) modify after https://github.com/Alluxio/alluxio/pull/12838
  }
}
