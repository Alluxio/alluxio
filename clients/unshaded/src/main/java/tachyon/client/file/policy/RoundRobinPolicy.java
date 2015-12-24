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

package tachyon.client.file.policy;

import java.util.List;

import tachyon.client.WorkerNetAddress;
import tachyon.client.block.BlockWorkerInfo;

/**
 * A policy that chooses the worker for the next block in a round-robin manner and skips workers
 * that do not have enough space. The policy returns null if no worker can be found.
 */
public final class RoundRobinPolicy implements FileWriteLocationPolicy {
  private List<BlockWorkerInfo> mWorkerInfoList;
  private int mIndex;

  /**
   * Constructs a new {@link RoundRobinPolicy}.
   *
   * @param workerInfoList the list of active worker information
   */
  public RoundRobinPolicy(List<BlockWorkerInfo> workerInfoList) {
    mWorkerInfoList = workerInfoList;
    mIndex = 0;
  }

  @Override
  public WorkerNetAddress getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) {
    // at most try all the workers
    for (int i = 0; i < mWorkerInfoList.size(); i ++) {
      WorkerNetAddress candidate = mWorkerInfoList.get(mIndex).getNetAddress();
      BlockWorkerInfo workerInfo = findBlockWorkerInfo(workerInfoList, candidate);
      mIndex = (mIndex + 1) % mWorkerInfoList.size();
      if (workerInfo == null
          || workerInfo.getCapacityBytes() - workerInfo.getUsedBytes() < blockSizeBytes) {
        continue;
      }
      return candidate;
    }
    return null;
  }

  /**
   * @param workerInfoList the list of worker info
   * @param address the address to look for
   * @return the worker info in the list that matches the host name, null if not found
   */
  private BlockWorkerInfo findBlockWorkerInfo(List<BlockWorkerInfo> workerInfoList,
      WorkerNetAddress address) {
    for (BlockWorkerInfo info : workerInfoList) {
      if (info.getNetAddress().equals(address)) {
        return info;
      }
    }
    return null;
  }
}
