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

package alluxio.client.block.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A policy that chooses the worker for the next block in a round-robin manner and skips workers
 * that do not have enough space.
 */
@NotThreadSafe
public final class RoundRobinPolicy implements BlockLocationPolicy {
  private List<BlockWorkerInfo> mWorkerInfoList;
  private int mIndex;
  private boolean mInitialized = false;
  /** This caches the {@link WorkerNetAddress} for the block IDs.*/
  private final HashMap<Long, WorkerNetAddress> mBlockLocationCache = new HashMap<>();

  /**
   * Constructs a new {@link RoundRobinPolicy}.
   *
   * @param conf Alluxio configuration
   */
  public RoundRobinPolicy(AlluxioConfiguration conf) {}

  /**
   * The policy uses the first fetch of worker info list as the base, and visits each of them in a
   * round-robin manner in the subsequent calls. The policy doesn't assume the list of worker info
   * in the subsequent calls has the same order from the first, and it will skip the workers that
   * are no longer active.
   *
   * Returns null if no worker can be found.
   *
   * @param options options
   * @return the address of the worker to write to
   */
  @Override
  @Nullable
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    Set<WorkerNetAddress> eligibleAddresses = new HashSet<>();
    for (BlockWorkerInfo info : options.getBlockWorkerInfos()) {
      eligibleAddresses.add(info.getNetAddress());
    }

    WorkerNetAddress address = mBlockLocationCache.get(options.getBlockInfo().getBlockId());
    if (address != null && eligibleAddresses.contains(address)) {
      return address;
    } else {
      address = null;
    }

    if (!mInitialized) {
      mWorkerInfoList = Lists.newArrayList(options.getBlockWorkerInfos());
      Collections.shuffle(mWorkerInfoList);
      mIndex = 0;
      mInitialized = true;
    }

    // at most try all the workers
    for (int i = 0; i < mWorkerInfoList.size(); i++) {
      WorkerNetAddress candidate = mWorkerInfoList.get(mIndex).getNetAddress();
      BlockWorkerInfo workerInfo = findBlockWorkerInfo(options.getBlockWorkerInfos(), candidate);
      mIndex = (mIndex + 1) % mWorkerInfoList.size();
      if (workerInfo != null
          && workerInfo.getCapacityBytes() >= options.getBlockInfo().getLength()
          && eligibleAddresses.contains(candidate)) {
        address = candidate;
        break;
      }
    }
    mBlockLocationCache.put(options.getBlockInfo().getBlockId(), address);
    return address;
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
    if (!(o instanceof RoundRobinPolicy)) {
      return false;
    }
    RoundRobinPolicy that = (RoundRobinPolicy) o;
    return Objects.equal(mWorkerInfoList, that.mWorkerInfoList)
        && Objects.equal(mIndex, that.mIndex)
        && Objects.equal(mInitialized, that.mInitialized)
        && Objects.equal(mBlockLocationCache, that.mBlockLocationCache);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWorkerInfoList, mIndex, mInitialized, mBlockLocationCache);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("workerInfoList", mWorkerInfoList)
        .add("index", mIndex)
        .add("initialized", mInitialized)
        .add("blockLocationCache", mBlockLocationCache)
        .toString();
  }
}
