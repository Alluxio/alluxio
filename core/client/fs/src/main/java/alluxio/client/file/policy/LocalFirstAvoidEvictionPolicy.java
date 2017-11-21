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
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;
/**
 * A policy that returns local host first, and if the local worker doesn't have enough availability,
 * it randomly picks a worker from the active workers list for each block write.
 * If no worker meets the demands, return local host.
 * USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES is used to reserve some space of the worker
 * to store the block, for the values mCapacityBytes minus mUsedBytes is not the available bytes.
 */
// TODO(peis): Move the BlockLocationPolicy implementation to alluxio.client.block.policy.
@ThreadSafe
public final class LocalFirstAvoidEvictionPolicy
    implements FileWriteLocationPolicy, BlockLocationPolicy {
  private final LocalFirstPolicy mPolicy;

  /**
   * Constructs a {@link LocalFirstAvoidEvictionPolicy}.
   */
  public LocalFirstAvoidEvictionPolicy() {
    mPolicy = new LocalFirstPolicy(true);
  }

  @Override
  public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) {
    return mPolicy.getWorkerForNextBlock(workerInfoList, blockSizeBytes);
  }

  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    return mPolicy.getWorker(options);
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
    return Objects.equal(mPolicy, that.mPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPolicy);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("policy", mPolicy)
        .toString();
  }
}
