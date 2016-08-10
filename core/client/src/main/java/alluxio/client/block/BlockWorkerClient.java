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

import alluxio.Client;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.ClientMetrics;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Interface for an Alluxio block worker client.
 */
public interface BlockWorkerClient extends Client {

  /**
   * @return the address of the worker
   */
  WorkerNetAddress getWorkerNetAddress();

  /**
   * Updates the latest block access time on the worker.
   *
   * @param blockId The id of the block
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  void accessBlock(final long blockId) throws ConnectionFailedException, IOException;

  /**
   * Notifies the worker to checkpoint the file asynchronously.
   *
   * @param fileId The id of the file
   * @return true if success, false otherwise
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  boolean asyncCheckpoint(final long fileId) throws IOException, AlluxioException;

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
   * Updates the session id of the client, starting a new session. The previous session's held
   * resources should have already been freed, and will be automatically freed after the timeout is
   * exceeded.
   *
   * @param newSessionId the new id that represents the new session
   */
  void createNewSession(long newSessionId);

  /**
   * @return the address of the worker's data server
   */
  InetSocketAddress getDataServerAddress();

  /**
   * @return the id of the session
   */
  long getSessionId();

  /**
   * @return true if the worker is local, false otherwise
   */
  boolean isLocal();

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
   * Connects to the worker.
   *
   * @throws IOException if a non-Alluxio exception occurs
   */
  // TODO(jiezhou): Consider merging the connect logic in this method into the super class.
  void connect() throws IOException;

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
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  boolean unlockBlock(final long blockId) throws ConnectionFailedException, IOException;

  /**
   * Sends a session heartbeat to the worker. This renews the client's lease on resources such as
   * locks and temporary files and updates the worker's metrics.
   *
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  void sessionHeartbeat() throws ConnectionFailedException, IOException;

  /**
   * Called only by {@link BlockWorkerClientHeartbeatExecutor}, encapsulates
   * {@link #sessionHeartbeat()} in order to cancel and cleanup the heartbeating thread in case of
   * failures.
   */
  void periodicHeartbeat();

  /**
   * Gets the client metrics of the worker.
   * @return the metrics of the worker
   */
  ClientMetrics getClientMetrics();
}
