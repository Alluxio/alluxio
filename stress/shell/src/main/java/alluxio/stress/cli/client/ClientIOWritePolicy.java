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

package alluxio.stress.cli.client;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A write type for restricting the writes to a set number of workers. This should only be used for
 * testing and benchmarking.
 */
@NotThreadSafe
public final class ClientIOWritePolicy implements BlockLocationPolicy {
  private static final AtomicInteger MAX_WORKERS = new AtomicInteger(1);

  private List<BlockWorkerInfo> mWorkerInfoList;
  private int mIndex;
  private volatile boolean mInitialized = false;

  /**
   * Updates the global state to reflect the max number of workers to write to. This must be set
   * before writes are performed to take effect.
   *
   * @param maxWorkers the max number of workers to write to
   */
  public static void setMaxWorkers(int maxWorkers) {
    MAX_WORKERS.set(maxWorkers);
  }

  /**
   * Constructs a new {@link ClientIOWritePolicy}.
   *
   * @param conf Alluxio configuration
   */
  public ClientIOWritePolicy(AlluxioConfiguration conf) {}

  /**
   *
   * @param options options
   * @return the address of the worker to write to
   */
  @Override
  @Nullable
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    Map<WorkerNetAddress, BlockWorkerInfo> eligibleWorkers = new HashMap<>();
    for (BlockWorkerInfo info : options.getBlockWorkerInfos()) {
      eligibleWorkers.put(info.getNetAddress(), info);
    }

    if (!mInitialized) {
      mWorkerInfoList = Lists.newArrayList(options.getBlockWorkerInfos());
      // sort by hashcode
      mWorkerInfoList.sort(Comparator.comparing(w -> w.getNetAddress().getHost()));
      // take the first subset
      mWorkerInfoList =
          mWorkerInfoList.subList(0, Math.min(MAX_WORKERS.get(), mWorkerInfoList.size()));
      if (mWorkerInfoList.size() < MAX_WORKERS.get()) {
        throw new IllegalStateException(String
            .format("Not enough eligible workers. expected: %d actual: %d", MAX_WORKERS.get(),
                mWorkerInfoList.size()));
      }
      mIndex = 0;
      mInitialized = true;
    }

    for (int i = 0; i < mWorkerInfoList.size(); i++) {
      WorkerNetAddress candidate = mWorkerInfoList.get(mIndex).getNetAddress();
      mIndex = (mIndex + 1) % mWorkerInfoList.size();

      BlockWorkerInfo workerInfo = eligibleWorkers.get(candidate);
      if (workerInfo != null && workerInfo.getCapacityBytes() >= options.getBlockInfo()
          .getLength()) {
        return candidate;
      }
    }

    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClientIOWritePolicy)) {
      return false;
    }
    ClientIOWritePolicy that = (ClientIOWritePolicy) o;
    return Objects.equal(mWorkerInfoList, that.mWorkerInfoList)
        && Objects.equal(mIndex, that.mIndex)
        && Objects.equal(mInitialized, that.mInitialized)
        ;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWorkerInfoList, mIndex, mInitialized);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("workerInfoList", mWorkerInfoList)
        .add("index", mIndex)
        .add("initialized", mInitialized)
        .toString();
  }
}
