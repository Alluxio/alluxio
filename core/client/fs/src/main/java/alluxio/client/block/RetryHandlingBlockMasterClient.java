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
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.master.MasterClientConfig;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.BlockMasterClientService;
import alluxio.thrift.GetBlockInfoTOptions;
import alluxio.thrift.GetBlockMasterInfoTOptions;
import alluxio.thrift.GetCapacityBytesTOptions;
import alluxio.thrift.GetUsedBytesTOptions;
import alluxio.thrift.GetWorkerInfoListTOptions;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.BlockMasterInfo.BlockMasterInfoField;
import alluxio.wire.ThriftUtils;
import alluxio.wire.WorkerInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
  private BlockMasterClientService.Client mClient = null;

  /**
   * Creates a new block master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingBlockMasterClient(MasterClientConfig conf) {
    super(conf);
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

  @Override
  public synchronized List<WorkerInfo> getWorkerInfoList() throws IOException {
    return retryRPC(() -> {
      List<WorkerInfo> result = new ArrayList<>();
      for (alluxio.thrift.WorkerInfo workerInfo : mClient
          .getWorkerInfoList(new GetWorkerInfoListTOptions()).getWorkerInfoList()) {
        result.add(ThriftUtils.fromThrift(workerInfo));
      }
      return result;
    });
  }

  @Override
  public synchronized List<WorkerInfo> getWorkerReport(
      final GetWorkerReportOptions options) throws IOException {
    return retryRPC(() -> {
      List<WorkerInfo> result = new ArrayList<>();
      for (alluxio.thrift.WorkerInfo workerInfo : mClient
          .getWorkerReport(options.toThrift()).getWorkerInfoList()) {
        result.add(ThriftUtils.fromThrift(workerInfo));
      }
      return result;
    });
  }

  /**
   * Returns the {@link BlockInfo} for a block id.
   *
   * @param blockId the block id to get the BlockInfo for
   * @return the {@link BlockInfo}
   */
  public synchronized BlockInfo getBlockInfo(final long blockId) throws IOException {
    return retryRPC(() ->
      ThriftUtils
          .fromThrift(mClient.getBlockInfo(blockId, new GetBlockInfoTOptions()).getBlockInfo())
    );
  }

  @Override
  public synchronized BlockMasterInfo getBlockMasterInfo(final Set<BlockMasterInfoField> fields)
      throws IOException {
    return retryRPC(() -> {
      Set<alluxio.thrift.BlockMasterInfoField> thriftFields = null;
      if (fields != null) {
        thriftFields = new HashSet<>();
        for (BlockMasterInfoField field : fields) {
          thriftFields.add(field.toThrift());
        }
      }
      return ThriftUtils.fromThrift(
          mClient.getBlockMasterInfo(new GetBlockMasterInfoTOptions(thriftFields))
              .getBlockMasterInfo());
    });
  }

  /**
   * Gets the total Alluxio capacity in bytes, on all the tiers of all the workers.
   *
   * @return total capacity in bytes
   */
  public synchronized long getCapacityBytes() throws IOException {
    return retryRPC(() ->
      mClient.getCapacityBytes(new GetCapacityBytesTOptions()).getBytes()
    );
  }

  /**
   * Gets the total amount of used space in bytes, on all the tiers of all the workers.
   *
   * @return amount of used space in bytes
   */
  public synchronized long getUsedBytes() throws IOException {
    return retryRPC(() ->
      mClient.getUsedBytes(new GetUsedBytesTOptions()).getBytes()
    );
  }
}
