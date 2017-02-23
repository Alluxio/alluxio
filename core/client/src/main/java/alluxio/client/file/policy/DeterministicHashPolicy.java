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

package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.master.block.BlockId;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This policy maps blockId to a deterministic worker. If the worker does not have enough
 * capacity to hold the block, it iterates the list of workers starting from the mapped index
 * until the worker with enough capacity is found.
 */
@NotThreadSafe
public final class DeterministicHashPolicy implements BlockLocationPolicy {
  private List<BlockWorkerInfo> mWorkerInfoList;
  private int mOffset;
  private boolean mInitialized = false;

  /**
   * Constructs a new {@link DeterministicHashPolicy}.
   */
  public DeterministicHashPolicy() {}

  @Override
  public WorkerNetAddress getWorkerForBlock(Iterable<BlockWorkerInfo> workerInfoList,
      long blockId, long blockSizeBytes) {
    if (!mInitialized) {
      mWorkerInfoList = Lists.newArrayList(workerInfoList);
      Collections.sort(mWorkerInfoList, new Comparator<BlockWorkerInfo>() {
        @Override
        public int compare(BlockWorkerInfo o1, BlockWorkerInfo o2) {
          return o1.getNetAddress().toString().compareToIgnoreCase(o2.getNetAddress().toString());
        }
      });
      mOffset = (int) (BlockId.getContainerId(blockId) % (long) mWorkerInfoList.size());
      mInitialized = true;
    }

    // Try the next one if the worker mapped from the blockId doesn't work untill all the workers
    // are examined.
    int index =
        (int) ((mOffset + BlockId.getSequenceNumber(blockId)) % (long) mWorkerInfoList.size());
    for (int i = 0; i < mWorkerInfoList.size(); i++) {
      WorkerNetAddress candidate = mWorkerInfoList.get(index).getNetAddress();
      BlockWorkerInfo workerInfo = findBlockWorkerInfo(workerInfoList, candidate);
      index = (index + 1) % mWorkerInfoList.size();
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
  private BlockWorkerInfo findBlockWorkerInfo(Iterable<BlockWorkerInfo> workerInfoList,
      WorkerNetAddress address) {
    for (BlockWorkerInfo info : workerInfoList) {
      if (info.getNetAddress().equals(address)) {
        return info;
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DeterministicHashPolicy)) {
      return false;
    }
    DeterministicHashPolicy that = (DeterministicHashPolicy) o;
    return Objects.equal(mWorkerInfoList, that.mWorkerInfoList)
        && Objects.equal(mOffset, that.mOffset)
        && Objects.equal(mInitialized, that.mInitialized);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWorkerInfoList, mOffset, mInitialized);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("workerInfoList", mWorkerInfoList)
        .add("offset", mOffset)
        .add("initialized", mInitialized)
        .toString();
  }
}
