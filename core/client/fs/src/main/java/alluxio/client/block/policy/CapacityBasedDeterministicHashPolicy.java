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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import org.apache.commons.codec.digest.MurmurHash3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A policy that pseudo-randomly distributes blocks between workers according to their capacity,
 * so that the probability a worker is chosen is equal to the ratio of its capacity over total
 * capacity of all workers, provided that the blocks requested follow a uniform distribution.
 * If sharding is disabled, the same block is always assigned to the same worker. If sharding
 * is enabled, the block is assigned to a fixed set of workers.
 *
 * The difference between this policy and {@link CapacityBaseRandomPolicy} is that this policy
 * uses the hashed block ID as the index to choose the target worker, so that the same block is
 * always routed to the same set of workers.
 *
 * Both this policy and {@link DeterministicHashPolicy} choose workers based the hashed block ID.
 * The difference is that {@link DeterministicHashPolicy} uniformly distributes the blocks among
 * the configured number of shards, while this policy chooses workers based on a distribution of
 * their normalized capacity.
 *
 * @see CapacityBaseRandomPolicy
 * @see DeterministicHashPolicy
 */
public class CapacityBasedDeterministicHashPolicy implements BlockLocationPolicy {
  private final int mShards;

  /**
   * Constructor required by
   * {@link BlockLocationPolicy.Factory#create(Class, AlluxioConfiguration)}.
   * @param conf Alluxio configuration
   */
  public CapacityBasedDeterministicHashPolicy(AlluxioConfiguration conf) {
    int numShards =
        conf.getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS);
    Preconditions.checkArgument(numShards >= 1, "number of shards must be no less than 1");
    mShards = numShards;
  }

  @Override
  public Optional<WorkerNetAddress> getWorker(GetWorkerOptions options) {
    // the target worker is determined by the following algorithm:
    // 1. build a cumulative distribution function by adding up all workers and their capacities.
    //    workers are sorted by their host name alphabetically.
    //    if worker A has 90 GB, B has 10 GB and C has 900 GB, the CDF looks like
    //    | 0 ... 90 | 90 ... 100 | 100 ... 1000 |
    //    | worker A |  worker B  |   worker C   |
    // 2. find a fixed starting point in [0, totalCapacity) determined by the hashed block id.
    //    | 0 ... 90 | 90 ... 100 | 100 ... 1000 |
    //    | worker A |  worker B  |   worker C   |
    //                     ^ start = 95
    // 3. find the corresponding worker in the CDF.
    //    which is worker B in this example
    // 4. if #shards = 1, this worker is selected. otherwise, find a set of candidates:
    //    4.1 hashed_block_id(0) = block id
    //    4.2 for i in [1, #shards], hashed_block_id(i) = hash(hashed_block_id(i-1))
    //    4.3 find the worker whose position corresponds to hashed_block_id(i) in the CDF,
    //        and add it to the candidates set
    //    4.4 repeat 4.2 - 4.4
    // 5. select a random worker in the candidate set
    TreeMap<Long, BlockWorkerInfo> capacityCdf = new TreeMap<>();
    AtomicLong totalCapacity = new AtomicLong(0);
    Streams.stream(options.getBlockWorkerInfos())
        .filter(workerInfo -> workerInfo.getCapacityBytes() >= options.getBlockInfo().getLength())
        // sort by hostname to guarantee two workers with the same capacity has a defined order
        .sorted(Comparator.comparing(w -> w.getNetAddress().getHost()))
        .forEach(workerInfo -> {
          capacityCdf.put(totalCapacity.get(), workerInfo);
          totalCapacity.getAndAdd(workerInfo.getCapacityBytes());
        });
    if (totalCapacity.get() == 0 || capacityCdf.isEmpty()) {
      return Optional.empty();
    }
    long blockId = options.getBlockInfo().getBlockId();
    BlockWorkerInfo chosenWorker = pickWorker(capacityCdf, blockId, totalCapacity.get());
    return Optional.of(chosenWorker.getNetAddress());
  }

  private BlockWorkerInfo pickWorker(TreeMap<Long, BlockWorkerInfo> capacityCdf,
      long blockId, long totalCapacity) {
    if (mShards == 1) {
      // if no sharding, simply return the worker corresponding to the start point
      long startPoint = Math.abs(hashBlockId(blockId)) % totalCapacity;
      return capacityCdf.floorEntry(startPoint).getValue();
    }
    long hashedBlockId = blockId;
    List<BlockWorkerInfo> candidates = new ArrayList<>();
    for (int i = 1; i <= Math.min(mShards, capacityCdf.size()); i++) {
      hashedBlockId = hashBlockId(hashedBlockId);
      BlockWorkerInfo candidate = capacityCdf
          .floorEntry(Math.abs(hashedBlockId) % totalCapacity) // non-null as capacities >= 0
          .getValue();
      candidates.add(candidate);
    }
    return getRandomCandidate(candidates);
  }

  @VisibleForTesting
  protected long hashBlockId(long blockId) {
    return MurmurHash3.hash64(blockId);
  }

  @VisibleForTesting
  protected BlockWorkerInfo getRandomCandidate(List<BlockWorkerInfo> candidates) {
    int randomIndex = ThreadLocalRandom.current().nextInt(candidates.size());
    return candidates.get(randomIndex);
  }
}
