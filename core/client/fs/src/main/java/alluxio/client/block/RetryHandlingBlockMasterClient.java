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
import alluxio.thrift.AlluxioService;
import alluxio.thrift.BlockMasterClientService;
import alluxio.thrift.GetBlockInfoTOptions;
import alluxio.thrift.GetCapacityBytesTOptions;
import alluxio.thrift.GetUsedBytesTOptions;
import alluxio.thrift.GetWorkerInfoListTOptions;
import alluxio.wire.BlockInfo;
import alluxio.wire.ThriftUtils;
import alluxio.wire.WorkerInfo;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

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
   * @param subject the parent subject, set to null if not present
   * @param masterAddress the master address
   */
  protected static RetryHandlingBlockMasterClient create(Subject subject,
      InetSocketAddress masterAddress) {
    return new RetryHandlingBlockMasterClient(subject, masterAddress);
  }

  private RetryHandlingBlockMasterClient(Subject subject, InetSocketAddress masterAddress) {
    super(subject, masterAddress);
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
  protected void afterConnect() {
    mClient = new BlockMasterClientService.Client(mProtocol);
  }

  /**
   * Gets the info of a list of workers.
   *
   * @return A list of worker info returned by master
   */
  public synchronized List<WorkerInfo> getWorkerInfoList() throws IOException {
    return retryRPC(new RpcCallable<List<WorkerInfo>>() {
      @Override
      public List<WorkerInfo> call() throws TException {
        List<WorkerInfo> result = new ArrayList<>();
        for (alluxio.thrift.WorkerInfo workerInfo : mClient
            .getWorkerInfoList(new GetWorkerInfoListTOptions()).getWorkerInfoList()) {
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
   */
  public synchronized BlockInfo getBlockInfo(final long blockId) throws IOException {
    return retryRPC(new RpcCallable<BlockInfo>() {
      @Override
      public BlockInfo call() throws TException {
        return ThriftUtils
            .fromThrift(mClient.getBlockInfo(blockId, new GetBlockInfoTOptions()).getBlockInfo());
      }
    });
  }

  /**
   * Gets the total Alluxio capacity in bytes, on all the tiers of all the workers.
   *
   * @return total capacity in bytes
   */
  public synchronized long getCapacityBytes() throws IOException {
    return retryRPC(new RpcCallable<Long>() {
      @Override
      public Long call() throws TException {
        return mClient.getCapacityBytes(new GetCapacityBytesTOptions()).getBytes();
      }
    });
  }

  /**
   * Gets the total amount of used space in bytes, on all the tiers of all the workers.
   *
   * @return amount of used space in bytes
   */
  public synchronized long getUsedBytes() throws IOException {
    return retryRPC(new RpcCallable<Long>() {
      @Override
      public Long call() throws TException {
        return mClient.getUsedBytes(new GetUsedBytesTOptions()).getBytes();
      }
    });
  }
}
