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

package alluxio.worker.block;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.BlockHeartbeatTOptions;
import alluxio.thrift.BlockMasterWorkerService;
import alluxio.thrift.Command;
import alluxio.thrift.CommitBlockTOptions;
import alluxio.thrift.GetWorkerIdTOptions;
import alluxio.thrift.RegisterWorkerTOptions;
import alluxio.wire.ThriftUtils;
import alluxio.wire.WorkerNetAddress;

import org.apache.thrift.TException;

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
  private BlockMasterWorkerService.Client mClient = null;

  /**
   * Creates a new instance of {@link BlockMasterClient} for the worker.
   *
   * @param masterAddress the master address
   */
  public BlockMasterClient(InetSocketAddress masterAddress) {
    super(null, masterAddress);
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
   */
  public synchronized void commitBlock(final long workerId, final long usedBytesOnTier,
      final String tierAlias, final long blockId, final long length) throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.commitBlock(workerId, usedBytesOnTier, tierAlias, blockId, length,
            new CommitBlockTOptions());
        return null;
      }
    });
  }

  /**
   * Returns a worker id for a workers net address.
   *
   * @param address the net address to get a worker id for
   * @return a worker id
   */
  public synchronized long getId(final WorkerNetAddress address) throws IOException {
    return retryRPC(new RpcCallable<Long>() {
      @Override
      public Long call() throws TException {
        return mClient.getWorkerId(ThriftUtils.toThrift(address), new GetWorkerIdTOptions())
            .getWorkerId();
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
   */
  public synchronized Command heartbeat(final long workerId,
      final Map<String, Long> usedBytesOnTiers, final List<Long> removedBlocks,
      final Map<String, List<Long>> addedBlocks) throws IOException {
    return retryRPC(new RpcCallable<Command>() {
      @Override
      public Command call() throws TException {
        return mClient.blockHeartbeat(workerId, usedBytesOnTiers, removedBlocks, addedBlocks,
            new BlockHeartbeatTOptions()).getCommand();
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
   */
  // TODO(yupeng): rename to workerBlockReport or workerInitialize?
  public synchronized void register(final long workerId, final List<String> storageTierAliases,
      final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
      final Map<String, List<Long>> currentBlocksOnTiers) throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.registerWorker(workerId, storageTierAliases, totalBytesOnTiers, usedBytesOnTiers,
            currentBlocksOnTiers, new RegisterWorkerTOptions());
        return null;
      }
    });
  }
}
