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
import java.util.concurrent.Future;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClient;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BlockMasterService;
import tachyon.thrift.WorkerInfo;

/**
 * The BlockMaster client, for clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety.
 */
// TODO: better deal with exceptions.
public final class BlockMasterClient extends MasterClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private BlockMasterService.Client mClient = null;

  // TODO: implement client heartbeat to the master
  private Future<?> mHeartbeat;

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
    // TODO: get a user id?
    // TODO: start client heartbeat thread, and submit it to the executor service
  }

  @Override
  protected void afterDisconnect() {
    // TODO: implement heartbeat cleanup
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
}
