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
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;
/**
 * A policy that returns the local worker first, and if the local worker doesn't
 * exist or have enough availability, will select the nearest worker from the active
 * workers list with sufficient availability.
 *
 * The definition of 'nearest worker' is based on {@link alluxio.wire.TieredIdentity}.
 * @see alluxio.wire.TieredIdentityUtils#nearest()
 *
 * The calculation of which worker gets selected is done for each block write.
 *
 * The {@link alluxio.conf.PropertyKey.USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES}
 * (alluxio.user.block.avoid.eviction.policy.reserved.size.bytes)
 * is used as buffer space on each worker when calculating available space
 * to store each block.
 */
@ThreadSafe
public final class LocalFirstAvoidEvictionPolicy implements BlockLocationPolicy {
  private final LocalFirstPolicy mPolicy;
  private final long mBlockCapacityReserved;

  /**
   * Constructs a new {@link LocalFirstAvoidEvictionPolicy}.
   *
   * @param conf Alluxio configuration
   */
  public LocalFirstAvoidEvictionPolicy(AlluxioConfiguration conf) {
    this(conf.getBytes(alluxio.conf.PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES),
        conf);
  }

  LocalFirstAvoidEvictionPolicy(long blockCapacityReserved,
      AlluxioConfiguration conf) {
    mPolicy = LocalFirstPolicy.create(conf);
    mBlockCapacityReserved = blockCapacityReserved;
  }

  @VisibleForTesting
  LocalFirstAvoidEvictionPolicy(long blockCapacityReserved, TieredIdentity identity,
      AlluxioConfiguration conf) {
    mPolicy = new LocalFirstPolicy(identity, conf);
    mBlockCapacityReserved = blockCapacityReserved;
  }

  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    List<BlockWorkerInfo> allWorkers = Lists.newArrayList(options.getBlockWorkerInfos());
    // Prefer workers with enough availability.
    List<BlockWorkerInfo> workers = allWorkers.stream()
        .filter(worker -> getAvailableBytes(worker) >= options.getBlockInfo().getLength())
        .collect(Collectors.toList());
    if (workers.isEmpty()) {
      workers = allWorkers;
    }
    GetWorkerOptions filteredWorkers = GetWorkerOptions.defaults()
        .setBlockInfo(options.getBlockInfo())
        .setBlockWorkerInfos(workers);
    return mPolicy.getWorker(filteredWorkers);
  }

  /**
   * Calculate the available bytes for a worker with the added buffer of
   * {@link alluxio.conf.PropertyKey.USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES}
   * (alluxio.user.block.avoid.eviction.policy.reserved.size.bytes)
   *
   * Since the information of BlockWorkerInfo is updated <em>after</em> a file
   * completes a write, mCapacityBytes minus mUsedBytes may not be the true available bytes.
   *
   * @param workerInfo BlockWorkerInfo of the worker
   * @return the available bytes of the worker
   */
  private long getAvailableBytes(BlockWorkerInfo workerInfo) {
    long mCapacityBytes = workerInfo.getCapacityBytes();
    long mUsedBytes = workerInfo.getUsedBytes();
    return mCapacityBytes - mUsedBytes - mBlockCapacityReserved;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LocalFirstAvoidEvictionPolicy)) {
      return false;
    }
    LocalFirstAvoidEvictionPolicy that = (LocalFirstAvoidEvictionPolicy) o;
    return Objects.equal(mPolicy, that.mPolicy)
        && Objects.equal(mBlockCapacityReserved, that.mBlockCapacityReserved);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPolicy);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("policy", mPolicy)
        .add("blockCapacityReservered", mBlockCapacityReserved)
        .toString();
  }
}
