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

import org.apache.thrift.TException;

import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.BlockMasterService;
import tachyon.thrift.Command;
import tachyon.thrift.NetAddress;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerInfo;

public class BlockMasterServiceHandler implements BlockMasterService.Iface {
  private final BlockMaster mBlockMaster;

  public BlockMasterServiceHandler(BlockMaster blockMaster) {
    mBlockMaster = blockMaster;
  }

  @Override
  public long workerGetWorkerId(NetAddress workerNetAddress) throws TException {
    return mBlockMaster.getWorkerId(workerNetAddress);
  }

  @Override
  public void workerRegister(long workerId, List<Long> totalBytesOnTiers,
      List<Long> usedBytesOnTiers, Map<Long, List<Long>> currentBlocksOnTiers)
      throws TachyonException, TException {
    mBlockMaster.workerRegister(workerId, totalBytesOnTiers, usedBytesOnTiers,
        currentBlocksOnTiers);
  }

  @Override
  public Command workerHeartbeat(long workerId, List<Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<Long, List<Long>> addedBlocksOnTiers)
      throws BlockInfoException, TException {
    return mBlockMaster.workerHeartbeat(workerId, usedBytesOnTiers, removedBlockIds,
        addedBlocksOnTiers);
  }

  @Override
  public void workerCommitBlock(long workerId, long usedBytesOnTier, int tier, long blockId,
      long length) throws BlockInfoException, TException {
    mBlockMaster.commitBlock(workerId, usedBytesOnTier, tier, blockId, length);
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() throws TException {
    return mBlockMaster.getWorkerInfoList();
  }

  @Override
  public long getCapacityBytes() throws TException {
    return mBlockMaster.getCapacityBytes();
  }

  @Override
  public long getUsedBytes() throws TException {
    return mBlockMaster.getUsedBytes();
  }

  @Override
  public BlockInfo getBlockInfo(long blockId) throws BlockInfoException {
    return mBlockMaster.getBlockInfo(blockId);
  }
}
