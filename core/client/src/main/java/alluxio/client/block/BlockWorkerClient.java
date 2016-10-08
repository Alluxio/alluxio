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

import alluxio.exception.AlluxioException;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Interface for an Alluxio block worker client.
 */
public interface BlockWorkerClient extends Closeable {

  /**
   * @return the address of the worker
   */
  WorkerNetAddress getWorkerNetAddress();

  /**
   * Updates the latest block access time on the worker.
   *
   * @param blockId The id of the block
   * @throws IOException if an I/O error occurs
   */
  void accessBlock(final long blockId) throws IOException;

  /**
   * Notifies the worker the block is cached.
   *
   * @param blockId The id of the block
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  void cacheBlock(final long blockId) throws IOException, AlluxioException;

  /**
   * Notifies worker that the block has been cancelled.
   *
   * @param blockId The Id of the block to be cancelled
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  void cancelBlock(final long blockId) throws IOException, AlluxioException;

  /**
   * @return the address of the worker's data server
   */
  InetSocketAddress getDataServerAddress();

  /**
   * @return the id of the session
   */
  long getSessionId();

  /**
   * Locks the block, therefore, the worker will not evict the block from the memory until it is
   * unlocked.
   *
   * @param blockId The id of the block
   * @return the path of the block file locked
   * @throws IOException if a non-Alluxio exception occurs
   */
  LockBlockResult lockBlock(final long blockId) throws IOException;

  /**
   * Promotes block back to the top StorageTier.
   *
   * @param blockId The id of the block that will be promoted
   * @return true if succeed, false otherwise
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  boolean promoteBlock(final long blockId) throws IOException, AlluxioException;

  /**
   * Gets temporary path for the block from the worker.
   *
   * @param blockId The id of the block
   * @param initialBytes The initial size bytes allocated for the block
   * @return the temporary path of the block
   * @throws IOException if a non-Alluxio exception occurs
   */
  String requestBlockLocation(final long blockId, final long initialBytes) throws IOException;

  /**
   * Requests space for some block from worker.
   *
   * @param blockId The id of the block
   * @param requestBytes The requested space size, in bytes
   * @return true if space was successfully allocated, false if the worker is unable to allocate
   *         space due to space exhaustion
   * @throws IOException if an exception occurs
   */
  boolean requestSpace(final long blockId, final long requestBytes) throws IOException;

  /**
   * Unlocks the block.
   *
   * @param blockId The id of the block
   * @return true if success, false otherwise
   * @throws IOException if an I/O error occurs
   */
  boolean unlockBlock(final long blockId) throws IOException;

  /**
   * Sends a session heartbeat to the worker. This renews the client's lease on resources such as
   * locks and temporary files.
   *
   * @throws IOException if an I/O error occurs
   * @throws InterruptedException if this thread is interrupted
   */
  void sessionHeartbeat() throws IOException, InterruptedException;

  /**
   * Closes the client.
   */
  @Override
  void close();
}
