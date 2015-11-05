/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BlockMasterService;
import tachyon.thrift.Command;
import tachyon.thrift.NetAddress;

/**
 * A wrapper for the thrift client to interact with the block master, used by tachyon worker.
 * <p/>
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
public final class WorkerBlockMasterClient extends MasterClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private BlockMasterService.Client mClient = null;

  /**
   * Creates a new block master client for the worker.
   *
   * @param masterAddress the master address
   * @param tachyonConf the Tachyon configuration
   */
  public WorkerBlockMasterClient(InetSocketAddress masterAddress, TachyonConf tachyonConf) {
    super(masterAddress, tachyonConf);
  }

  @Override
  protected String getServiceName() {
    return Constants.BLOCK_MASTER_SERVICE_NAME;
  }

  @Override
  protected void afterConnect() {
    mClient = new BlockMasterService.Client(mProtocol);
  }

  /**
   * Commits a block on a worker.
   *
   * @param workerId the worker id committing the block
   * @param usedBytesOnTier the amount of used bytes on the tier the block is committing to
   * @param tierAlias the alias of the tier the block is being committed to
   * @param blockId the block id being committed
   * @param length the length of the block being committed
   * @throws IOException if an I/O error occurs
   */
  public synchronized void commitBlock(long workerId, long usedBytesOnTier, String tierAlias,
      long blockId, long length) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        mClient.workerCommitBlock(workerId, usedBytesOnTier, tierAlias, blockId, length);
        return;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Returns a worker id for a workers net address.
   *
   * @param address the net address to get a worker id for
   * @return a worker id
   * @throws IOException if an I/O error occurs
   */
  // TODO(yupeng): rename to workerRegister?
  public synchronized long getId(NetAddress address) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.workerGetWorkerId(address);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * The method the worker should periodically execute to heartbeat back to the master.
   *
   * @param workerId the worker id
   * @param usedBytesOnTiers a mapping from storage tier alias to used bytes
   * @param removedBlocks a list of block removed from this worker
   * @param addedBlocks a mapping from storage tier alias to added blocks
   * @return an optional command for the worker to execute
   * @throws IOException if an I/O error occurs
   */
  public synchronized Command heartbeat(long workerId, Map<String, Long> usedBytesOnTiers,
      List<Long> removedBlocks, Map<String, List<Long>> addedBlocks) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.workerHeartbeat(workerId, usedBytesOnTiers, removedBlocks, addedBlocks);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * The method the worker should execute to register with the block master.
   *
   * @param workerId the worker id of the worker registering
   * @param storageTierAliases a list of storage tier aliases in ordinal order
   * @param totalBytesOnTiers mapping from storage tier alias to total bytes
   * @param usedBytesOnTiers mapping from storage tier alias to used bytes
   * @param currentBlocksOnTiers mapping from storage tier alias to the list of list of blocks
   * @throws IOException if an I/O error occurs or the workerId doesn't exist
   */
  // TODO(yupeng): rename to workerBlockReport or workerInitialize?
  public synchronized void register(long workerId, List<String> storageTierAliases,
      Map<String, Long> totalBytesOnTiers, Map<String, Long> usedBytesOnTiers,
      Map<String, List<Long>> currentBlocksOnTiers) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        mClient.workerRegister(workerId, storageTierAliases, totalBytesOnTiers, usedBytesOnTiers,
            currentBlocksOnTiers);
        return;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }
}
