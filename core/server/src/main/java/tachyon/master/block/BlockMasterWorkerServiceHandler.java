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

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.exception.TachyonException;
import tachyon.thrift.BlockMasterWorkerService;
import tachyon.thrift.Command;
import tachyon.thrift.TachyonTException;
import tachyon.thrift.WorkerNetAddress;
import tachyon.wire.ThriftUtils;

/**
 * This class is a Thrift handler for block master RPCs invoked by a Tachyon worker.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. TACHYON-1664)
public class BlockMasterWorkerServiceHandler implements BlockMasterWorkerService.Iface {
  private final BlockMaster mBlockMaster;

  /**
   * Creates a new instance of {@link BlockMasterWorkerServiceHandler}.
   *
   * @param blockMaster the {@link BlockMaster} the handler uses internally
   */
  public BlockMasterWorkerServiceHandler(BlockMaster blockMaster) {
    Preconditions.checkNotNull(blockMaster);
    mBlockMaster = blockMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.BLOCK_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  public long getWorkerId(WorkerNetAddress workerNetAddress) {
    return mBlockMaster.getWorkerId(ThriftUtils.fromThrift((workerNetAddress)));
  }

  @Override
  public void registerWorker(long workerId, List<String> storageTiers,
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
  public Command heartbeat(long workerId, Map<String, Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<String, List<Long>> addedBlocksOnTiers) {
    return mBlockMaster.workerHeartbeat(workerId, usedBytesOnTiers, removedBlockIds,
        addedBlocksOnTiers);
  }

  @Override
  public void commitBlock(long workerId, long usedBytesOnTier, String tierAlias,
      long blockId, long length) {
    mBlockMaster.commitBlock(workerId, usedBytesOnTier, tierAlias, blockId, length);
  }
}
