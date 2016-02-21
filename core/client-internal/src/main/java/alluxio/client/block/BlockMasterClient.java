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

package alluxio.client.block;

import alluxio.AbstractMasterClient;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockMasterClientService;
import alluxio.wire.BlockInfo;
import alluxio.wire.ThriftUtils;
import alluxio.wire.WorkerInfo;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the block master, used by alluxio clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class BlockMasterClient extends AbstractMasterClient {
  private BlockMasterClientService.Client mClient = null;

  /**
   * Creates a new block master client.
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
        for (alluxio.thrift.WorkerInfo workerInfo : mClient.getWorkerInfoList()) {
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
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized BlockInfo getBlockInfo(final long blockId)
      throws AlluxioException, IOException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<BlockInfo>() {
      @Override
      public BlockInfo call() throws AlluxioTException, TException {
        return ThriftUtils.fromThrift(mClient.getBlockInfo(blockId));
      }
    });
  }

  /**
   * Gets the total Alluxio capacity in bytes, on all the tiers of all the workers.
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
