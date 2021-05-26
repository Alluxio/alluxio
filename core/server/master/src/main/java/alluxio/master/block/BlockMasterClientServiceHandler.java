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

package alluxio.master.block;

import alluxio.ServerRpcUtils;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.BlockMasterClientServiceGrpc;
import alluxio.grpc.BlockMasterInfo;
import alluxio.grpc.BlockMasterInfoField;
import alluxio.grpc.GetBlockInfoPOptions;
import alluxio.grpc.GetBlockInfoPRequest;
import alluxio.grpc.GetBlockInfoPResponse;
import alluxio.grpc.GetBlockMasterInfoPOptions;
import alluxio.grpc.GetBlockMasterInfoPResponse;
import alluxio.grpc.GetCapacityBytesPOptions;
import alluxio.grpc.GetCapacityBytesPResponse;
import alluxio.grpc.GetUsedBytesPOptions;
import alluxio.grpc.GetUsedBytesPResponse;
import alluxio.grpc.GetWorkerInfoListPOptions;
import alluxio.grpc.GetWorkerInfoListPResponse;
import alluxio.grpc.GetWorkerLostStoragePOptions;
import alluxio.grpc.GetWorkerLostStoragePResponse;
import alluxio.grpc.GetWorkerReportPOptions;
import alluxio.grpc.GrpcUtils;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * This class is a gRPC handler for block master RPCs invoked by an Alluxio client.
 */
public final class BlockMasterClientServiceHandler
    extends BlockMasterClientServiceGrpc.BlockMasterClientServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMasterClientServiceHandler.class);
  private static final long RPC_RESPONSE_SIZE_WARNING_THRESHOLD =
      ServerConfiguration.getBytes(PropertyKey.MASTER_RPC_RESPONSE_SIZE_WARNING_THRESHOLD);

  private final BlockMaster mBlockMaster;

  /**
   * Creates a new instance of {@link BlockMasterClientServiceHandler}.
   *
   * @param blockMaster the {@link BlockMaster} the handler uses internally
   */
  public BlockMasterClientServiceHandler(BlockMaster blockMaster) {
    Preconditions.checkNotNull(blockMaster, "blockMaster");
    mBlockMaster = blockMaster;
  }

  @Override
  public void getBlockInfo(GetBlockInfoPRequest request,
      StreamObserver<GetBlockInfoPResponse> responseObserver) {
    long blockId = request.getBlockId();
    GetBlockInfoPOptions options = request.getOptions();
    ServerRpcUtils.call(LOG,
        () -> GetBlockInfoPResponse.newBuilder()
            .setBlockInfo(GrpcUtils.toProto(mBlockMaster.getBlockInfo(blockId))).build(),
        "GetBlockInfo", "blockId=%s, options=%s", responseObserver, blockId, options);
  }

  @Override
  public void getBlockMasterInfo(GetBlockMasterInfoPOptions options,
      StreamObserver<GetBlockMasterInfoPResponse> responseObserver) {
    ServerRpcUtils.call(LOG, () -> {
      BlockMasterInfo.Builder infoBuilder = BlockMasterInfo.newBuilder();
      for (BlockMasterInfoField field : (options.getFiltersCount() != 0)
          ? options.getFiltersList()
          : Arrays.asList(BlockMasterInfoField.values())) {
        switch (field) {
          case CAPACITY_BYTES:
            infoBuilder.setCapacityBytes(mBlockMaster.getCapacityBytes());
            break;
          case CAPACITY_BYTES_ON_TIERS:
            infoBuilder.putAllCapacityBytesOnTiers(mBlockMaster.getTotalBytesOnTiers());
            break;
          case FREE_BYTES:
            infoBuilder.setFreeBytes(mBlockMaster.getCapacityBytes() - mBlockMaster.getUsedBytes());
            break;
          case LIVE_WORKER_NUM:
            infoBuilder.setLiveWorkerNum(mBlockMaster.getWorkerCount());
            break;
          case LOST_WORKER_NUM:
            infoBuilder.setLostWorkerNum(mBlockMaster.getLostWorkerCount());
            break;
          case USED_BYTES:
            infoBuilder.setUsedBytes(mBlockMaster.getUsedBytes());
            break;
          case USED_BYTES_ON_TIERS:
            infoBuilder.putAllUsedBytesOnTiers(mBlockMaster.getUsedBytesOnTiers());
            break;
          default:
            LOG.warn("Unrecognized block master info field: " + field);
        }
      }
      return GetBlockMasterInfoPResponse.newBuilder().setBlockMasterInfo(infoBuilder).build();
    }, "GetBlockMasterInfo", "options=%s", responseObserver, options);
  }

  @Override
  public void getCapacityBytes(GetCapacityBytesPOptions options,
      StreamObserver<GetCapacityBytesPResponse> responseObserver) {
    ServerRpcUtils.call(LOG,
        () -> GetCapacityBytesPResponse.newBuilder()
            .setBytes(mBlockMaster.getCapacityBytes()).build(),
        "GetCapacityBytes", "options=%s", responseObserver, options);
  }

  @Override
  public void getUsedBytes(GetUsedBytesPOptions options,
      StreamObserver<GetUsedBytesPResponse> responseObserver) {
    ServerRpcUtils.call(LOG,
        () -> GetUsedBytesPResponse.newBuilder().setBytes(mBlockMaster.getUsedBytes()).build(),
        "GetUsedBytes", "options=%s", responseObserver, options);
  }

  @Override
  public void getWorkerInfoList(GetWorkerInfoListPOptions options,
      StreamObserver<GetWorkerInfoListPResponse> responseObserver) {
    ServerRpcUtils.call(LOG,
        () -> {
          GetWorkerInfoListPResponse response = GetWorkerInfoListPResponse.newBuilder()
              .addAllWorkerInfos(mBlockMaster.getWorkerInfoList().stream().map(GrpcUtils::toProto)
                  .collect(Collectors.toList())).build();
          if (response.getSerializedSize() > RPC_RESPONSE_SIZE_WARNING_THRESHOLD) {
            LOG.warn("getWorkerInfoList response has size {}, {} WorkerInfo",
                response.getSerializedSize(), response.getWorkerInfosCount());
          }
          return response;
        }, "GetWorkerInfoList", "options=%s", responseObserver, options);
  }

  @Override
  public void getWorkerReport(GetWorkerReportPOptions options,
      StreamObserver<GetWorkerInfoListPResponse> responseObserver) {
    ServerRpcUtils.call(LOG,
        () -> {
          GetWorkerInfoListPResponse response = GetWorkerInfoListPResponse.newBuilder()
              .addAllWorkerInfos(mBlockMaster.getWorkerReport(new GetWorkerReportOptions(options))
                  .stream().map(GrpcUtils::toProto).collect(Collectors.toList())).build();
          if (response.getSerializedSize() > RPC_RESPONSE_SIZE_WARNING_THRESHOLD) {
            LOG.warn("getWorkerReport response has size {}, {} WorkerInfo",
                response.getSerializedSize(), response.getWorkerInfosCount());
          }
          return response;
        }, "GetWorkerReport", "options=%s", responseObserver, options);
  }

  @Override
  public void getWorkerLostStorage(GetWorkerLostStoragePOptions options,
      StreamObserver<GetWorkerLostStoragePResponse> responseObserver) {
    ServerRpcUtils.call(LOG,
        () -> GetWorkerLostStoragePResponse.newBuilder()
            .addAllWorkerLostStorageInfo(mBlockMaster.getWorkerLostStorage()).build(),
        "GetWorkerLostStorage", "options=%s", responseObserver, options);
  }
}
