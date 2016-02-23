/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.AbstractMasterClient;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockMasterWorkerService;
import alluxio.thrift.Command;
import alluxio.wire.WorkerNetAddress;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the block master, used by alluxio worker.
 * <p/>
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class BlockMasterClient extends AbstractMasterClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private BlockMasterWorkerService.Client mClient = null;

  /**
   * Creates a new instance of {@link BlockMasterClient} for the worker.
   *
   * @param masterAddress the master address
   * @param configuration the Alluxio configuration
   */
  public BlockMasterClient(InetSocketAddress masterAddress, Configuration configuration) {
    super(masterAddress, configuration);
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.BLOCK_MASTER_WORKER_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.BLOCK_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new BlockMasterWorkerService.Client(mProtocol);
  }

  /**
   * Commits a block on a worker.
   *
   * @param workerId the worker id committing the block
   * @param usedBytesOnTier the amount of used bytes on the tier the block is committing to
   * @param tierAlias the alias of the tier the block is being committed to
   * @param blockId the block id being committed
   * @param length the length of the block being committed
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  public synchronized void commitBlock(final long workerId, final long usedBytesOnTier,
      final String tierAlias, final long blockId, final long length)
          throws IOException, ConnectionFailedException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.commitBlock(workerId, usedBytesOnTier, tierAlias, blockId, length);
        return null;
      }
    });
  }

  /**
   * Returns a worker id for a workers net address.
   *
   * @param address the net address to get a worker id for
   * @return a worker id
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getId(final WorkerNetAddress address)
      throws IOException, ConnectionFailedException {
    return retryRPC(new RpcCallable<Long>() {
      @Override
      public Long call() throws TException {
        return mClient.getWorkerId(new alluxio.thrift.WorkerNetAddress(address.getHost(),
            address.getRpcPort(), address.getDataPort(), address.getWebPort()));
      }
    });
  }

  /**
   * The method the worker should periodically execute to heartbeat back to the master.
   *
   * @param workerId the worker id
   * @param usedBytesOnTiers a mapping from storage tier alias to used bytes
   * @param removedBlocks a list of block removed from this worker
   * @param addedBlocks a mapping from storage tier alias to added blocks
   * @return an optional command for the worker to execute
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  public synchronized Command heartbeat(final long workerId,
      final Map<String, Long> usedBytesOnTiers, final List<Long> removedBlocks,
      final Map<String, List<Long>> addedBlocks) throws IOException, ConnectionFailedException {
    return retryRPC(new RpcCallable<Command>() {
      @Override
      public Command call() throws TException {
        return mClient.heartbeat(workerId, usedBytesOnTiers, removedBlocks, addedBlocks);
      }
    });
  }

  /**
   * The method the worker should execute to register with the block master.
   *
   * @param workerId the worker id of the worker registering
   * @param storageTierAliases a list of storage tier aliases in ordinal order
   * @param totalBytesOnTiers mapping from storage tier alias to total bytes
   * @param usedBytesOnTiers mapping from storage tier alias to used bytes
   * @param currentBlocksOnTiers mapping from storage tier alias to the list of list of blocks
   * @throws AlluxioException if registering the worker fails
   * @throws IOException if an I/O error occurs or the workerId doesn't exist
   */
  // TODO(yupeng): rename to workerBlockReport or workerInitialize?
  public synchronized void register(final long workerId, final List<String> storageTierAliases,
      final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
      final Map<String, List<Long>> currentBlocksOnTiers) throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.registerWorker(workerId, storageTierAliases, totalBytesOnTiers, usedBytesOnTiers,
            currentBlocksOnTiers);
        return null;
      }
    });
  }
}
