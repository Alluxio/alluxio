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

package alluxio.client.block;

import alluxio.AbstractMasterClient;
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
public final class RetryHandlingBlockMasterClient extends AbstractMasterClient
    implements BlockMasterClient {
  private BlockMasterClientService.Client mClient;

  /**
   * Creates a new block master client.
   *
   * @param masterAddress the master address
   */
  public RetryHandlingBlockMasterClient(InetSocketAddress masterAddress) {
    super(masterAddress);
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
        List<WorkerInfo> result = new ArrayList<>();
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
