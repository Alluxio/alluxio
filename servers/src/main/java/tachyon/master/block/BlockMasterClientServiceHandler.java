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

package tachyon.master.block;

import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.exception.TachyonException;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockMasterClientService;
import tachyon.thrift.TachyonTException;
import tachyon.thrift.WorkerInfo;

/**
 * This class is a Thrift handler for block master RPCs invoked by a Tachyon client.
 */
public class BlockMasterClientServiceHandler implements BlockMasterClientService.Iface {
  private final BlockMaster mBlockMaster;

  /**
   * Creates a new instance of {@link BlockMasterClientServiceHandler}.
   *
   * @param blockMaster the {@BlockMaster} the handler uses internally
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
    return mBlockMaster.getWorkerInfoList();
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
  public BlockInfo getBlockInfo(long blockId) throws TachyonTException {
    try {
      return mBlockMaster.getBlockInfo(blockId);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }
}
