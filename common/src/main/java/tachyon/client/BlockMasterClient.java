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
import java.util.concurrent.ExecutorService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockMasterService;
import tachyon.thrift.Command;
import tachyon.thrift.NetAddress;
import tachyon.thrift.WorkerInfo;

/**
 * The BlockMaster client, for clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety.
 */
// TODO: better deal with exceptions.
public final class BlockMasterClient extends MasterClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private BlockMasterService.Client mClient = null;

  public BlockMasterClient(InetSocketAddress masterAddress, ExecutorService executorService,
      TachyonConf tachyonConf) {
    super(masterAddress, executorService, tachyonConf);
  }

  @Override
  protected String getServiceName() {
    return Constants.BLOCK_MASTER_SERVICE_NAME;
  }

  @Override
  protected void afterConnect() {
    mClient = new BlockMasterService.Client(mProtocol);
  }

  @Override
  protected void afterDisconnect() {
  }

  /**
   * Get the info of a list of workers.
   *
   * @return A list of worker info returned by master
   * @throws IOException
   */
  public synchronized List<WorkerInfo> getWorkerInfoList() throws IOException {
    while (!mIsClosed) {
      connect();

      try {
        return mClient.getWorkerInfoList();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  public synchronized BlockInfo getBlockInfo(long blockId) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getBlockInfo(blockId);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  /**
   * Get the total capacity in bytes.
   *
   * @return capacity in bytes
   * @throws IOException
   */
  public synchronized long getCapacityBytes() throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getCapacityBytes();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }

  /**
   * Get the amount of used space in bytes.
   *
   * @return amount of used space in bytes
   * @throws IOException
   */
  public synchronized long getUsedBytes() throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getUsedBytes();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }

  public synchronized void workerCommitBlock(long workerId, long usedBytesOnTier, int tier, long
      blockId, long length) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        mClient.workerCommitBlock(workerId, usedBytesOnTier, tier, blockId, length);
        return;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
  }

  public synchronized long workerGetId(NetAddress address) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.workerGetWorkerId(address);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1L;
  }

  public synchronized Command workerHeartbeat(long workerId, List<Long> usedBytesOnTiers, List<Long>
      removedBlocks, Map<Long, List<Long>> addedBlocks) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.workerHeartbeat(workerId, usedBytesOnTiers, removedBlocks, addedBlocks);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  public synchronized long workerRegister(long workerId, List<Long> totalBytesOnTiers, List<Long>
      usedBytesOnTiers, Map<Long, List<Long>> currentBlocksOnTiers) throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.workerRegister(workerId, totalBytesOnTiers, usedBytesOnTiers,
            currentBlocksOnTiers);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }
}
