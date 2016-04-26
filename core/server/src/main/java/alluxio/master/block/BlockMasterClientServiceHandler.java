/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockInfo;
import alluxio.thrift.BlockMasterClientService;
import alluxio.thrift.WorkerInfo;
import alluxio.wire.ThriftUtils;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for block master RPCs invoked by an Alluxio client.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public class BlockMasterClientServiceHandler implements BlockMasterClientService.Iface {
  private final BlockMaster mBlockMaster;

  /**
   * Creates a new instance of {@link BlockMasterClientServiceHandler}.
   *
   * @param blockMaster the {@link BlockMaster} the handler uses internally
   */
  public BlockMasterClientServiceHandler(BlockMaster blockMaster) {
    Preconditions.checkNotNull(blockMaster);
    mBlockMaster = blockMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() {
    List<WorkerInfo> workerInfos = new ArrayList<WorkerInfo>();
    for (alluxio.wire.WorkerInfo workerInfo : mBlockMaster.getWorkerInfoList()) {
      workerInfos.add(ThriftUtils.toThrift(workerInfo));
    }
    return workerInfos;
  }

  @Override
  public long getCapacityBytes() {
    return mBlockMaster.getCapacityBytes();
  }

  @Override
  public long getUsedBytes() {
    return mBlockMaster.getUsedBytes();
  }

  @Override
  public BlockInfo getBlockInfo(final long blockId) throws AlluxioTException {
    return RpcUtils.call(new RpcCallable<BlockInfo>() {
      @Override
      public BlockInfo call() throws AlluxioException {
        return ThriftUtils.toThrift(mBlockMaster.getBlockInfo(blockId));
      }
    });
  }
}
