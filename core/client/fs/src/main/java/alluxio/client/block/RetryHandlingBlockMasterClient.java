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
import alluxio.grpc.BlockMasterClientServiceGrpc;
import alluxio.grpc.GetBlockInfoPRequest;
import alluxio.grpc.GetBlockMasterInfoPOptions;
import alluxio.grpc.GetCapacityBytesPOptions;
import alluxio.grpc.GetUsedBytesPOptions;
import alluxio.grpc.GetWorkerInfoListPOptions;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientConfig;
import alluxio.grpc.GrpcUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.BlockMasterInfo.BlockMasterInfoField;
import alluxio.wire.WorkerInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
  private BlockMasterClientServiceGrpc.BlockMasterClientServiceBlockingStub mClient = null;

  /**
   * Creates a new block master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingBlockMasterClient(MasterClientConfig conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.BLOCK_MASTER_CLIENT_SERVICE;
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
    mClient = BlockMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public synchronized List<WorkerInfo> getWorkerInfoList() throws IOException {
    return retryRPC(() -> {
      List<WorkerInfo> result = new ArrayList<>();
      for (alluxio.grpc.WorkerInfo workerInfo : mClient
          .getWorkerInfoList(GetWorkerInfoListPOptions.getDefaultInstance())
          .getWorkerInfosList()) {
        result.add(GrpcUtils.fromProto(workerInfo));
      }
      return result;
    });
  }

  @Override
  public synchronized List<WorkerInfo> getWorkerReport(final GetWorkerReportOptions options)
      throws IOException {
    return retryRPC(() -> {
      List<WorkerInfo> result = new ArrayList<>();
      for (alluxio.grpc.WorkerInfo workerInfo : mClient.getWorkerReport(options.toProto())
          .getWorkerInfosList()) {
        result.add(GrpcUtils.fromProto(workerInfo));
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
    return retryRPC(() -> {
      return GrpcUtils.fromProto(
          mClient.getBlockInfo(GetBlockInfoPRequest.newBuilder().setBlockId(blockId).build())
              .getBlockInfo());
    });
  }

  @Override
  public synchronized BlockMasterInfo getBlockMasterInfo(final Set<BlockMasterInfoField> fields)
      throws IOException {
    return retryRPC(() -> {
      return BlockMasterInfo
          .fromProto(mClient.getBlockMasterInfo(GetBlockMasterInfoPOptions.newBuilder()
              .addAllFilters(
                  fields.stream().map(BlockMasterInfoField::toProto).collect(Collectors.toList()))
              .build()).getBlockMasterInfo());
    });
  }

  /**
   * Gets the total Alluxio capacity in bytes, on all the tiers of all the workers.
   *
   * @return total capacity in bytes
   */
  public synchronized long getCapacityBytes() throws IOException {
    return retryRPC(() -> mClient
        .getCapacityBytes(GetCapacityBytesPOptions.getDefaultInstance()).getBytes());
  }

  /**
   * Gets the total amount of used space in bytes, on all the tiers of all the workers.
   *
   * @return amount of used space in bytes
   */
  public synchronized long getUsedBytes() throws IOException {
    return retryRPC(
        () -> mClient.getUsedBytes(GetUsedBytesPOptions.getDefaultInstance()).getBytes());
  }
}
