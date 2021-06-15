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
import alluxio.conf.PropertyKey;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This policy maps the blockId to several deterministic Alluxio workers. The number of workers a
 * block can be mapped to can be passed through the constructor. The default is 1. It skips the
 * workers that do not have enough capacity to hold the block.
 *
 * This policy is useful for limiting the amount of replication that occurs when reading blocks from
 * the UFS with high concurrency. With 30 workers and 100 remote clients reading the same block
 * concurrently, the replication level for the block would get close to 30 as each worker reads
 * and caches the block for one or more clients. If the clients use DeterministicHashPolicy with
 * 3 shards, the 100 clients will split their reads between just 3 workers, so that the replication
 * level for the block will be only 3 when the data is first loaded.
 *
 * Note that the hash function relies on the number of workers in the cluster, so if the number of
 * workers changes, the workers chosen by the policy for a given block will likely change.
 */
@NotThreadSafe
public final class DeterministicHashPolicy implements BlockLocationPolicy {
  /** The default number of shards to serve a block. */
  private final int mShards;
  private final Random mRandom = new Random();
  private final HashFunction mHashFunc = Hashing.md5();

  /**
   * Constructs a new {@link DeterministicHashPolicy}.
   *
   * @param conf Alluxio configuration
   */
  public DeterministicHashPolicy(AlluxioConfiguration conf) {
    int numShards =
        conf.getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS);
    Preconditions.checkArgument(numShards >= 1);
    mShards = numShards;
  }

  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    List<BlockWorkerInfo> workerInfos = Lists.newArrayList(options.getBlockWorkerInfos());
    workerInfos.sort((o1, o2) ->
        o1.getNetAddress().toString().compareToIgnoreCase(o2.getNetAddress().toString()));
    HashMap<WorkerNetAddress, BlockWorkerInfo> blockWorkerInfoMap = new HashMap<>();
    for (BlockWorkerInfo workerInfo : options.getBlockWorkerInfos()) {
      blockWorkerInfoMap.put(workerInfo.getNetAddress(), workerInfo);
    }

    List<WorkerNetAddress> workers = new ArrayList<>();
    // Try the next one if the worker mapped from the blockId doesn't work until all the workers
    // are examined.
    int hv =
        Math.abs(mHashFunc.newHasher().putLong(options.getBlockInfo().getBlockId()).hash().asInt());
    int index = hv % workerInfos.size();
    for (BlockWorkerInfo blockWorkerInfoUnused : workerInfos) {
      WorkerNetAddress candidate = workerInfos.get(index).getNetAddress();
      BlockWorkerInfo workerInfo = blockWorkerInfoMap.get(candidate);
      if (workerInfo != null
          && workerInfo.getCapacityBytes() >= options.getBlockInfo().getLength()) {
        workers.add(candidate);
        if (workers.size() >= mShards) {
          break;
        }
      }
      index = (index + 1) % workerInfos.size();
    }
    return workers.isEmpty() ? null : workers.get(mRandom.nextInt(workers.size()));
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
    return Objects.equal(mShards, that.mShards);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mShards);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("shards", mShards).toString();
  }
}
