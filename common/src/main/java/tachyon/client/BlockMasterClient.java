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
   * @param tachyonConf the Tachyon configuration
   */
  public BlockMasterClient(InetSocketAddress masterAddress, TachyonConf tachyonConf) {
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
   * Gets the info of a list of workers.
   *
   * @return A list of worker info returned by master
   * @throws IOException if an I/O error occurs
   */
  public synchronized List<WorkerInfo> getWorkerInfoList() throws IOException {
    return retryRPC(new RpcCallable<List<WorkerInfo>>() {
      @Override
      public List<WorkerInfo> call() throws TException {
        return mClient.getWorkerInfoList();
      }
    });
  }

  /**
   * Returns the BlockInfo for a block id.
   *
   * @param blockId the block id to get the BlockInfo for
   * @return the BlockInfo
   * @throws IOException if an I/O error occurs
   */
  public synchronized BlockInfo getBlockInfo(final long blockId) throws IOException {
    return retryRPC(new RpcCallable<BlockInfo>() {
      @Override
      public BlockInfo call() throws TException {
        return mClient.getBlockInfo(blockId);
      }
    });
  }

  /**
   * Gets the total Tachyon capacity in bytes, on all the tiers of all the workers.
   *
   * @return total capacity in bytes
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getCapacityBytes() throws IOException {
    return retryRPC(new RpcCallable<Long>() {
      @Override
      public Long call() throws TException {
        return mClient.getCapacityBytes();
      }
    });
  }

  /**
   * Gets the total amount of used space in bytes, on all the tiers of all the workers.
   *
   * @return amount of used space in bytes
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getUsedBytes() throws IOException {
    return retryRPC(new RpcCallable<Long>() {
      @Override
      public Long call() throws TException {
        return mClient.getUsedBytes();
      }
    });
  }
}
