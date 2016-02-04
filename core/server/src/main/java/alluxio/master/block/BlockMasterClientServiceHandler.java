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

package alluxio.master.block;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.thrift.BlockInfo;
import alluxio.thrift.BlockMasterClientService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.WorkerInfo;
import alluxio.wire.ThriftUtils;

/**
 * This class is a Thrift handler for block master RPCs invoked by a Alluxio client.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. TACHYON-1664)
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
  public BlockInfo getBlockInfo(long blockId) throws AlluxioTException {
    try {
      return ThriftUtils.toThrift(mBlockMaster.getBlockInfo(blockId));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }
}
