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
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This policy maps blockId to several deterministic Alluxio workers. The number of workers a block
 * can be mapped to can be passed through the constructor. The default is 1. It skips the workers
 * that do not have enough capacity to hold the block.
 */
@NotThreadSafe
public final class DeterministicHashPolicy implements BlockLocationPolicy {
  /** The default number of shards to serve a block. */
  private static final int DEFAULT_NUM_SHARDS = 1;
  private final int mShards;
  private final Random mRandom = new Random();
  private List<BlockWorkerInfo> mWorkerInfoList;
  private boolean mInitialized = false;

  /**
   * Constructs a new {@link DeterministicHashPolicy}.
   */
  public DeterministicHashPolicy() {
    this(DEFAULT_NUM_SHARDS);
  }

  /**
   * Constructs a new {@link DeterministicHashPolicy}.
   *
   * @param numShards the number of shards a block's traffic can be sharded to
   */
  public DeterministicHashPolicy(Integer numShards) {
    Preconditions.checkArgument(numShards >= 1);
    mShards = numShards;
  }

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
      mInitialized = true;
    }

    List<WorkerNetAddress> workers = new ArrayList<>();
    // Try the next one if the worker mapped from the blockId doesn't work until all the workers
    // are examined.
    int index = (int) (blockId % (long) mWorkerInfoList.size());
    for (int i = 0; i < mWorkerInfoList.size(); i++) {
      WorkerNetAddress candidate = mWorkerInfoList.get(index).getNetAddress();
      BlockWorkerInfo workerInfo = findBlockWorkerInfo(workerInfoList, candidate);
      if (workerInfo != null && workerInfo.getCapacityBytes() >= blockSizeBytes) {
        workers.add(candidate);
        if (workers.size() >= mShards) {
          break;
        }
      }
      index = (index + 1) % mWorkerInfoList.size();
    }
    return workers.isEmpty() ? null : workers.get(mRandom.nextInt(workers.size()));
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
        && Objects.equal(mInitialized, that.mInitialized)
        && Objects.equal(mShards, that.mShards);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWorkerInfoList, mInitialized, mShards);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("workerInfoList", mWorkerInfoList)
        .add("initialized", mInitialized)
        .add("shards", mShards)
        .toString();
  }
}
