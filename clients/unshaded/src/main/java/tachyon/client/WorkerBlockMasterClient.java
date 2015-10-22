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
import tachyon.exception.TachyonException;
import tachyon.thrift.BlockMasterService;
import tachyon.thrift.Command;
import tachyon.thrift.NetAddress;
import tachyon.thrift.TachyonTException;

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
   * @param tier the tier the block is being committed to
   * @param blockId the block id being committed
   * @param length the length of the block being committed
   * @throws IOException if an I/O error occurs
   */
  public synchronized void commitBlock(final long workerId, final long usedBytesOnTier,
      final int tier, final long blockId, final long length) throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.workerCommitBlock(workerId, usedBytesOnTier, tier, blockId, length);
        return null;
      }
    });
  }

  /**
   * Returns a worker id for a workers net address.
   *
   * @param address the net address to get a worker id for
   * @return a worker id
   * @throws IOException if an I/O error occurs
   */
  // TODO: rename to workerRegister?
  public synchronized long getId(final NetAddress address) throws IOException {
    return retryRPC(new RpcCallable<Long>() {
      @Override
      public Long call() throws TException {
        return mClient.workerGetWorkerId(address);
      }
    });
  }

  /**
   * The method the worker should periodically execute to heartbeat back to the master.
   *
   * @param workerId the worker id
   * @param usedBytesOnTiers a list of used bytes on each tier
   * @param removedBlocks a list of block removed from this worker
   * @param addedBlocks the added blocks for each storage dir. It maps storage dir id, to a list of
   *        added block for that storage dir.
   * @return an optional command for the worker to execute
   * @throws IOException if an I/O error occurs
   */
  public synchronized Command heartbeat(final long workerId, final List<Long> usedBytesOnTiers,
      final List<Long> removedBlocks, final Map<Long, List<Long>> addedBlocks) throws IOException {
    return retryRPC(new RpcCallable<Command>() {
      @Override
      public Command call() throws TException {
        return mClient.workerHeartbeat(workerId, usedBytesOnTiers, removedBlocks, addedBlocks);
      }
    });
  }

  /**
   * The method the worker should execute to register with the block master.
   *
   * @param workerId the worker id of the worker registering
   * @param totalBytesOnTiers list of total bytes on each tier
   * @param usedBytesOnTiers list of the used byes on each tier
   * @param currentBlocksOnTiers a mapping of each storage dir, to all the blocks on that storage
   *        dir
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  // TODO: rename to workerBlockReport or workerInitialize?
  public synchronized void register(final long workerId, final List<Long> totalBytesOnTiers,
      final List<Long> usedBytesOnTiers, final Map<Long, List<Long>> currentBlocksOnTiers)
          throws TachyonException, IOException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.workerRegister(workerId, totalBytesOnTiers, usedBytesOnTiers, currentBlocksOnTiers);
        return null;
      }
    });
  }
}
