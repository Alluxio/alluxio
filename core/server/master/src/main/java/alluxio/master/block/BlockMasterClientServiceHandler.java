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

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallable;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockMasterClientService;
import alluxio.thrift.BlockMasterInfo;
import alluxio.thrift.BlockMasterInfoField;
import alluxio.thrift.GetBlockInfoTOptions;
import alluxio.thrift.GetBlockInfoTResponse;
import alluxio.thrift.GetBlockMasterInfoTOptions;
import alluxio.thrift.GetBlockMasterInfoTResponse;
import alluxio.thrift.GetCapacityBytesTOptions;
import alluxio.thrift.GetCapacityBytesTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.GetUsedBytesTOptions;
import alluxio.thrift.GetUsedBytesTResponse;
import alluxio.thrift.GetWorkerInfoListTOptions;
import alluxio.thrift.GetWorkerInfoListTResponse;
import alluxio.thrift.GetWorkerReportTOptions;
import alluxio.thrift.WorkerInfo;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for block master RPCs invoked by an Alluxio client.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class BlockMasterClientServiceHandler implements BlockMasterClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMasterClientServiceHandler.class);

  private final BlockMaster mBlockMaster;

  /**
   * Creates a new instance of {@link BlockMasterClientServiceHandler}.
   *
   * @param blockMaster the {@link BlockMaster} the handler uses internally
   */
  BlockMasterClientServiceHandler(BlockMaster blockMaster) {
    Preconditions.checkNotNull(blockMaster, "blockMaster");
    mBlockMaster = blockMaster;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public GetCapacityBytesTResponse getCapacityBytes(GetCapacityBytesTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallable<GetCapacityBytesTResponse>) () ->
        new GetCapacityBytesTResponse(mBlockMaster.getCapacityBytes()),
        "GetCapacityBytes", "GetCapacityBytes: options=%s", options);
  }

  @Override
  public GetUsedBytesTResponse getUsedBytes(GetUsedBytesTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallable<GetUsedBytesTResponse>) () -> new GetUsedBytesTResponse(
        mBlockMaster.getUsedBytes()), "GetUsedBytes", "GetUsedBytes: options=%s", options);
  }

  @Override
  public GetBlockInfoTResponse getBlockInfo(final long blockId, GetBlockInfoTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG,
        (RpcCallableThrowsIOException<GetBlockInfoTResponse>) () -> new GetBlockInfoTResponse(
            mBlockMaster.getBlockInfo(blockId).toThrift()), "GetBlockInfo",
        "GetBlockInfo: blockId=%s, options=%s", blockId, options);
  }

  @Override
  public GetBlockMasterInfoTResponse getBlockMasterInfo(final GetBlockMasterInfoTOptions options)
      throws TException {
    return RpcUtils.call((RpcUtils.RpcCallable<GetBlockMasterInfoTResponse>) () -> {
      BlockMasterInfo info = new alluxio.thrift.BlockMasterInfo();
      for (BlockMasterInfoField field : options.getFilter() != null ? options.getFilter()
          : Arrays.asList(BlockMasterInfoField.values())) {
        switch (field) {
          case CAPACITY_BYTES:
            info.setCapacityBytes(mBlockMaster.getCapacityBytes());
            break;
          case CAPACITY_BYTES_ON_TIERS:
            info.setCapacityBytesOnTiers(mBlockMaster.getTotalBytesOnTiers());
            break;
          case FREE_BYTES:
            info.setFreeBytes(mBlockMaster.getCapacityBytes() - mBlockMaster.getUsedBytes());
            break;
          case LIVE_WORKER_NUM:
            info.setLiveWorkerNum(mBlockMaster.getWorkerCount());
            break;
          case LOST_WORKER_NUM:
            info.setLostWorkerNum(mBlockMaster.getLostWorkerCount());
            break;
          case USED_BYTES:
            info.setUsedBytes(mBlockMaster.getUsedBytes());
            break;
          case USED_BYTES_ON_TIERS:
            info.setUsedBytesOnTiers(mBlockMaster.getUsedBytesOnTiers());
            break;
          default:
            LOG.warn("Unrecognized block master info field: " + field);
        }
      }
      return new GetBlockMasterInfoTResponse(info);
    });
  }

  @Override
  public GetWorkerInfoListTResponse getWorkerInfoList(GetWorkerInfoListTOptions options)
      throws AlluxioTException {
    return RpcUtils.call((RpcCallableThrowsIOException<GetWorkerInfoListTResponse>) () -> {
      List<WorkerInfo> workerInfos = new ArrayList<>();
      for (alluxio.wire.WorkerInfo workerInfo : mBlockMaster.getWorkerInfoList()) {
        workerInfos.add(workerInfo.toThrift());
      }
      return new GetWorkerInfoListTResponse(workerInfos);
    });
  }

  @Override
  public GetWorkerInfoListTResponse getWorkerReport(GetWorkerReportTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<GetWorkerInfoListTResponse>) () -> {
      List<WorkerInfo> workerInfos = new ArrayList<>();
      for (alluxio.wire.WorkerInfo workerInfo : mBlockMaster
          .getWorkerReport(new GetWorkerReportOptions(options))) {
        workerInfos.add(workerInfo.toThrift());
      }
      return new GetWorkerInfoListTResponse(workerInfos);
    }, "GetWorkerReport", "GetWorkerReport: options=%s", options);
  }
}
