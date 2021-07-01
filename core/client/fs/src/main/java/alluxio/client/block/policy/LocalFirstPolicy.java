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
import alluxio.client.block.util.BlockLocationUtils;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.network.TieredIdentityFactory;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
 */
@ThreadSafe
public final class LocalFirstPolicy implements BlockLocationPolicy {
  private final TieredIdentity mTieredIdentity;
  private final AlluxioConfiguration mConf;

  /**
   * Constructs a new {@link LocalFirstPolicy}.
   *
   * @param conf Alluxio configuration
   */
  public LocalFirstPolicy(AlluxioConfiguration conf) {
    mTieredIdentity = TieredIdentityFactory.localIdentity(conf);
    mConf = conf;
  }

  static LocalFirstPolicy create(AlluxioConfiguration conf) {
    return new LocalFirstPolicy(conf);
  }

  @VisibleForTesting
  LocalFirstPolicy(TieredIdentity identity, AlluxioConfiguration conf) {
    mTieredIdentity = identity;
    mConf = conf;
  }

  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    List<BlockWorkerInfo> shuffledWorkers = Lists.newArrayList(options.getBlockWorkerInfos());
    Collections.shuffle(shuffledWorkers);
    // Workers must have enough capacity to hold the block.
    List<BlockWorkerInfo> candidateWorkers = shuffledWorkers.stream()
        .filter(worker -> worker.getCapacityBytes() >= options.getBlockInfo().getLength())
        .collect(Collectors.toList());

    // Try finding the nearest worker.
    List<WorkerNetAddress> addresses = candidateWorkers.stream()
        .map(worker -> worker.getNetAddress())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    Optional<Pair<WorkerNetAddress, Boolean>> nearest =
        BlockLocationUtils.nearest(mTieredIdentity, addresses, mConf);
    if (!nearest.isPresent()) {
      return null;
    }
    return nearest.get().getFirst();
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
    return Objects.equals(mTieredIdentity, that.mTieredIdentity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mTieredIdentity);
  }

  @Override
  public String toString() {
    return com.google.common.base.MoreObjects.toStringHelper(this)
        .add("tieredIdentity", mTieredIdentity)
        .toString();
  }
}
