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

import java.io.IOException;

/**
 * An interface for worker internal client to interact with worker operations directly
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
   * This operation must be paired with {@link #cleanBlockReader(long, long, BlockReader)}.
   *
   * @param request the block read request
   * @return a block reader to read data from
   */
  BlockReader getBlockReader(BlockReadRequest request) throws IOException,
      BlockDoesNotExistException, InvalidWorkerStateException,
      BlockAlreadyExistsException, WorkerOutOfSpaceException;

  /**
   * Cleans data reader and related blocks after using the block reader obtained
   * from {@link #getBlockReader(BlockReadRequest)}.
   *
   * @param sessionId the session id which used for getting the block reader
   * @param blockId the block id this block reader belongs to
   * @param reader the to be cleaned block reader
   */
  void cleanBlockReader(long sessionId, long blockId, BlockReader reader)
      throws IOException, BlockAlreadyExistsException, WorkerOutOfSpaceException;

  /**
   * Moves a block from its current location to a target location, currently only tier level moves
   * are supported. Throws an {@link IllegalArgumentException} if the tierAlias is out of range of
   * tiered storage.
   *
   * @param blockId the id of the block to move
   * @param tierAlias the alias of the tier to move the block to
   * @throws BlockDoesNotExistException if blockId cannot be found
   * @throws BlockAlreadyExistsException if blockId already exists in committed blocks of the
   *         newLocation
   * @throws InvalidWorkerStateException if blockId has not been committed
   * @throws WorkerOutOfSpaceException if newLocation does not have enough extra space to hold the
   *         block
   */
  void moveBlock(long blockId, String tierAlias)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException;

  /**
   * Frees a block from Alluxio managed space.
   *
   * @param blockId the id of the block to be freed
   * @throws InvalidWorkerStateException if blockId has not been committed
   * @throws BlockDoesNotExistException if block cannot be found
   */
  void removeBlock(long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException;
}
