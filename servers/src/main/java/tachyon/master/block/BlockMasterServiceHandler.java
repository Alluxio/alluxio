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
import java.util.Map;

import tachyon.exception.TachyonException;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockMasterService;
import tachyon.thrift.Command;
import tachyon.thrift.NetAddress;
import tachyon.thrift.TachyonTException;
import tachyon.thrift.WorkerInfo;

public class BlockMasterServiceHandler implements BlockMasterService.Iface {
  private final BlockMaster mBlockMaster;

  public BlockMasterServiceHandler(BlockMaster blockMaster) {
    mBlockMaster = blockMaster;
  }

  @Override
  public long workerGetWorkerId(NetAddress workerNetAddress) {
    return mBlockMaster.getWorkerId(workerNetAddress);
  }

  @Override
  public void workerRegister(long workerId, List<String> storageTiers,
      Map<String, Long> totalBytesOnTiers, Map<String, Long> usedBytesOnTiers,
      Map<String, List<Long>> currentBlocksOnTiers) throws TachyonTException {
    try {
      mBlockMaster.workerRegister(workerId, storageTiers, totalBytesOnTiers,
          usedBytesOnTiers, currentBlocksOnTiers);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public Command workerHeartbeat(long workerId, Map<String, Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<String, List<Long>> addedBlocksOnTiers) {
    return mBlockMaster.workerHeartbeat(workerId, usedBytesOnTiers, removedBlockIds,
        addedBlocksOnTiers);
  }

  @Override
  public void workerCommitBlock(long workerId, long usedBytesOnTier, String tierAlias,
      long blockId, long length) {
    mBlockMaster.commitBlock(workerId, usedBytesOnTier, tierAlias, blockId, length);
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
