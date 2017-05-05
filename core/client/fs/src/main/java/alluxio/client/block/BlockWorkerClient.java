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

package alluxio.client.block;

import alluxio.QuietlyCloseable;
import alluxio.client.block.options.LockBlockOptions;
import alluxio.client.resource.LockBlockResource;
import alluxio.retry.RetryPolicy;
import alluxio.wire.WorkerNetAddress;

/**
 * Interface for an Alluxio block worker client.
 */
public interface BlockWorkerClient extends QuietlyCloseable {

  /**
   * Factory for {@link BlockWorkerClient}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory method for {@link BlockWorkerClient}.
     *
     * @param clientPool the client pool
     * @param clientHeartbeatPool the client pool for heartbeat
     * @param workerNetAddress the worker address to connect to
     * @param sessionId the session id to use, this should be unique
     * @return new {@link BlockWorkerClient} instance
     */
    public static BlockWorkerClient create(BlockWorkerThriftClientPool clientPool,
        BlockWorkerThriftClientPool clientHeartbeatPool, WorkerNetAddress workerNetAddress,
        Long sessionId) {
      return RetryHandlingBlockWorkerClient.create(clientPool, clientHeartbeatPool,
          workerNetAddress, sessionId);
    }
  }

  /**
   * Updates the latest block access time on the worker.
   *
   * @param blockId the ID of the block
   */
  void accessBlock(final long blockId);

  /**
   * Notifies the worker the block is cached.
   *
   * @param blockId the ID of the block
   */
  void cacheBlock(final long blockId);

  /**
   * Notifies worker that the block has been cancelled.
   *
   * @param blockId the ID of the block to be cancelled
   */
  void cancelBlock(final long blockId);

  /**
   * @return the ID of the session
   */
  long getSessionId();

  /**
   * @return the address of the worker
   */
  WorkerNetAddress getWorkerNetAddress();

  /**
   * Locks the block, therefore, the worker will not evict the block from the memory until it is
   * unlocked.
   *
   * @param blockId the ID of the block
   * @param options the lock block options
   * @return the lock block result
   */
  LockBlockResource lockBlock(final long blockId, final LockBlockOptions options);

  /**
   * A wrapper over {@link BlockWorkerClient#lockBlock(long, LockBlockOptions)} to lock a block
   * that is not in Alluxio but in UFS. It retries if it fails to lock because of contention for
   * the block on the worker.
   *
   * @param blockId the block ID
   * @param options the lock block options
   * @return the lock block result
   */
  LockBlockResource lockUfsBlock(final long blockId, final LockBlockOptions options);

  /**
   * Promotes block back to the top StorageTier.
   *
   * @param blockId the ID of the block that will be promoted
   * @return true if succeed, false otherwise
   */
  boolean promoteBlock(final long blockId);

  /**
   * Removes a block from the internal storage of this worker.
   *
   * @param blockId the ID of the block that will be removed
   */
  void removeBlock(final long blockId);

  /**
   * Gets temporary path for the block from the worker.
   *
   * @param blockId the ID of the block
   * @param initialBytes the initial size bytes allocated for the block
   * @param tier the target tier
   * @return the temporary path of the block
   */
  String requestBlockLocation(final long blockId, final long initialBytes, final int tier);

  /**
   * Requests space for some block from worker.
   *
   * @param blockId the ID of the block
   * @param requestBytes the requested space size, in bytes
   * @return true if space was successfully allocated, false if the worker is unable to allocate
   *         space due to space exhaustion
   */
  boolean requestSpace(final long blockId, final long requestBytes);

  /**
   * Unlocks the block.
   *
   * @param blockId the ID of the block
   * @return true if success, false otherwise
   */
  boolean unlockBlock(final long blockId);

  /**
   * Sends a session heartbeat to the worker. This renews the client's lease on resources such as
   * locks and temporary files.
   *
   * @param retryPolicy the retry policy to use
   */
  void sessionHeartbeat(RetryPolicy retryPolicy);
}
