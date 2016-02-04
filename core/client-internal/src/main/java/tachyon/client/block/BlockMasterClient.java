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

package tachyon.client.block;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.exception.TachyonException;
import tachyon.thrift.BlockMasterClientService;
import tachyon.thrift.TachyonService;
import tachyon.thrift.TachyonTException;
import tachyon.wire.BlockInfo;
import tachyon.wire.ThriftUtils;
import tachyon.wire.WorkerInfo;

/**
 * A wrapper for the thrift client to interact with the block master, used by tachyon clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class BlockMasterClient extends MasterClientBase {
  private BlockMasterClientService.Client mClient = null;

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
  protected TachyonService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new BlockMasterClientService.Client(mProtocol);
  }

  /**
   * Gets the info of a list of workers.
   *
   * @return A list of worker info returned by master
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  public synchronized List<WorkerInfo> getWorkerInfoList()
      throws IOException, ConnectionFailedException {
    return retryRPC(new RpcCallable<List<WorkerInfo>>() {
      @Override
      public List<WorkerInfo> call() throws TException {
        List<WorkerInfo> result = new ArrayList<WorkerInfo>();
        for (tachyon.thrift.WorkerInfo workerInfo : mClient.getWorkerInfoList()) {
          result.add(ThriftUtils.fromThrift(workerInfo));
        }
        return result;
      }
    });
  }

  /**
   * Returns the {@link BlockInfo} for a block id.
   *
   * @param blockId the block id to get the BlockInfo for
   * @return the {@link BlockInfo}
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized BlockInfo getBlockInfo(final long blockId)
      throws TachyonException, IOException {
    return retryRPC(new RpcCallableThrowsTachyonTException<BlockInfo>() {
      @Override
      public BlockInfo call() throws TachyonTException, TException {
        return ThriftUtils.fromThrift(mClient.getBlockInfo(blockId));
      }
    });
  }

  /**
   * Gets the total Tachyon capacity in bytes, on all the tiers of all the workers.
   *
   * @return total capacity in bytes
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getCapacityBytes() throws ConnectionFailedException, IOException {
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
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getUsedBytes() throws ConnectionFailedException, IOException {
    return retryRPC(new RpcCallable<Long>() {
      @Override
      public Long call() throws TException {
        return mClient.getUsedBytes();
      }
    });
  }
}
