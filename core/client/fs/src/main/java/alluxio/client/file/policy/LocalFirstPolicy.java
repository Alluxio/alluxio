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
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.network.TieredIdentityFactory;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A policy that returns local host first, and if the local worker doesn't have enough availability,
 * it randomly picks a worker from the active workers list for each block write.
 */
// TODO(peis): Move the BlockLocationPolicy implementation to alluxio.client.block.policy.
@ThreadSafe
public final class LocalFirstPolicy implements FileWriteLocationPolicy, BlockLocationPolicy {
  private final String mLocalHostName;
  private final TieredIdentity mTieredIdentity;

  /**
   * Constructs a {@link LocalFirstPolicy}.
   */
  public LocalFirstPolicy() {
    this(TieredIdentityFactory.getInstance());
  }

  /**
   * @param localTieredIdentity the local tiered identity
   */
  @VisibleForTesting
  LocalFirstPolicy(TieredIdentity localTieredIdentity) {
    mLocalHostName = NetworkAddressUtils.getClientHostName();
    mTieredIdentity = localTieredIdentity;
  }

  @Override
  @Nullable
  public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) {
    List<BlockWorkerInfo> shuffledWorkers = Lists.newArrayList(workerInfoList);
    Collections.shuffle(shuffledWorkers);
    List<WorkerNetAddress> enoughCapacityWorkers = shuffledWorkers.stream()
        .filter(worker -> worker.getCapacityBytes() >= blockSizeBytes)
        .map(BlockWorkerInfo::getNetAddress)
        .collect(Collectors.toList());

    // Try finding a worker based on nearest tiered identity.
    List<TieredIdentity> identities = enoughCapacityWorkers.stream()
        .map(worker -> worker.getTieredIdentity())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    Optional<TieredIdentity> nearest = mTieredIdentity.nearest(identities);
    if (!nearest.isPresent()) {
      return null;
    }
    // Map back to the address with the nearest tiered identity.
    return enoughCapacityWorkers.stream()
        .filter(worker -> worker.getTieredIdentity() == nearest.get())
        .findFirst().orElse(null);
  }

  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    return getWorkerForNextBlock(options.getBlockWorkerInfos(), options.getBlockSize());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LocalFirstPolicy)) {
      return false;
    }
    LocalFirstPolicy that = (LocalFirstPolicy) o;
    return Objects.equals(mLocalHostName, that.mLocalHostName)
        && Objects.equals(mTieredIdentity, that.mTieredIdentity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mLocalHostName, mTieredIdentity);
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
        .add("localHostName", mLocalHostName)
        .add("tieredIdentity", mTieredIdentity)
        .toString();
  }
}
