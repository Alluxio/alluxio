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

import java.io.IOException;

/**
 * An interface for worker internal clients to interact with worker operations directly
 * without going through external RPC frameworks.
 */
public interface LocalBlockWorker {

  /**
   * Submits the async cache request to the worker to execute.
   *
   * @param request the async cache request
   */
  void asyncCache(AsyncCacheRequest request);

  /**
   * Gets the block reader to read from Alluxio block or UFS block.
   * This operation must be paired with {@link #cleanBlockReader(BlockReader, BlockReadRequest)}.
   *
   * @param request the block read request
   * @return a block reader to read data from
   * @throws BlockAlreadyExistsException if it fails to commit the block to Alluxio block store
   *         because the block exists in the Alluxio block store after opening the ufs block reader
   * @throws BlockDoesNotExistException if the requested block does not exist in this worker
   * @throws InvalidWorkerStateException if blockId does not belong to sessionId
   * @throws WorkerOutOfSpaceException if there is no enough space
   * @throws IOException if it fails to get block reader
   */
  BlockReader getBlockReader(BlockReadRequest request) throws IOException,
      BlockDoesNotExistException, InvalidWorkerStateException,
      BlockAlreadyExistsException, WorkerOutOfSpaceException;

  /**
   * Cleans data reader and related blocks after using the block reader obtained
   * from {@link #getBlockReader(BlockReadRequest)}.
   *
   * @param reader to be cleaned block reader
   * @param request the block read request
   * @throws BlockAlreadyExistsException if it fails to commit the block to Alluxio block store
   *         because the block exists in the Alluxio block store when closing the ufs block
   * @throws WorkerOutOfSpaceException if there is not enough space
   * @throws IOException if it fails to get block reader
   */
  void cleanBlockReader(BlockReader reader, BlockReadRequest request)
      throws IOException, BlockAlreadyExistsException, WorkerOutOfSpaceException;
}
