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
import java.util.concurrent.ExecutorService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockMasterService;
import tachyon.thrift.WorkerInfo;

/**
 * A wrapper for the thrift client to interact with the block master, used by tachyon clients.
 *
 * TODO(jiri): The functions in this wrapper contain very similar boilerplate. It would make sense
 * to have a single "Retry" utility is used to to execute the while () { try ... catch ... } logic,
 * parametrized by the RPC to invoke.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
public final class BlockMasterClient extends MasterClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private BlockMasterService.Client mClient = null;

  /**
   * Creates a new block master client.
   *
   * @param masterAddress the master address
   * @param executorService the executor service
   * @param tachyonConf the Tachyon configuration
   */
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

  /**
   * Gets the info of a list of workers.
   *
   * @return A list of worker info returned by master
   * @throws IOException if an I/O error occurs
   */
  public synchronized List<WorkerInfo> getWorkerInfoList() throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getWorkerInfoList();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Returns the BlockInfo for a block id.
   *
   * @param blockId the block id to get the BlockInfo for
   * @return the BlockInfo
   * @throws IOException if an I/O error occurs
   */
  public synchronized BlockInfo getBlockInfo(long blockId) throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getBlockInfo(blockId);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Gets the total Tachyon capacity in bytes, on all the tiers of all the workers.
   *
   * @return total capacity in bytes
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getCapacityBytes() throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getCapacityBytes();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Gets the total amount of used space in bytes, on all the tiers of all the workers.
   *
   * @return amount of used space in bytes
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getUsedBytes() throws IOException {
    int retry = 0;
    while (!mClosed && (retry ++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return mClient.getUsedBytes();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }
}
