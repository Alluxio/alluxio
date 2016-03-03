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

package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A policy that chooses the worker for the next block in a round-robin manner and skips workers
 * that do not have enough space. The policy returns null if no worker can be found.
 */
@NotThreadSafe
public final class RoundRobinPolicy implements FileWriteLocationPolicy {
  private List<BlockWorkerInfo> mWorkerInfoList;
  private int mIndex;
  private boolean mInitialized = false;

  /**
   * The policy uses the first fetch of worker info list as the base, and visits each of them in a
   * round-robin manner in the subsequent calls. The policy doesn't assume the list of worker info
   * in the subsequent calls has the same order from the first, and it will skip the workers that
   * are no longer active.
   *
   * @param workerInfoList the info of the active workers
   * @param blockSizeBytes the size of the block in bytes
   * @return the address of the worker to write to
   */
  @Override
  public WorkerNetAddress getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) {
    if (!mInitialized) {
      mWorkerInfoList = workerInfoList;
      Collections.shuffle(mWorkerInfoList);
      mIndex = 0;
      mInitialized = true;
    }

    // at most try all the workers
    for (int i = 0; i < mWorkerInfoList.size(); i++) {
      WorkerNetAddress candidate = mWorkerInfoList.get(mIndex).getNetAddress();
      BlockWorkerInfo workerInfo = findBlockWorkerInfo(workerInfoList, candidate);
      mIndex = (mIndex + 1) % mWorkerInfoList.size();
      if (workerInfo != null && workerInfo.getCapacityBytes() >= blockSizeBytes) {
        return candidate;
      }
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
