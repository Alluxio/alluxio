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

import org.apache.commons.codec.digest.MurmurHash3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Randomly distribute workload based on the worker capacities so bigger workers get more requests.
 * The randomness is based on the capacity instead of availability because in the long run,
 * all workers will be filled up and have availability close to 0.
 * We do not want the policy to degenerate to all workers having the same chance.
 */
@ThreadSafe
public class CapacityBaseRandomPolicy implements BlockLocationPolicy {
  private final int mMaxReplicaSize;

  /**
   * Constructs a new {@link CapacityBaseRandomPolicy}
   * needed for instantiation in {@link BlockLocationPolicy.Factory}.
   *
   * @param conf Alluxio configuration
   */
  public CapacityBaseRandomPolicy(AlluxioConfiguration conf) {
    mMaxReplicaSize = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MAX);
  }

  @Override
  public Optional<WorkerNetAddress> getWorker(GetWorkerOptions options) {
    List<BlockWorkerInfo> sortedBlockWorkerList = toSortedList(options.getBlockWorkerInfos());
    // All the capacities will form a ring of continuous intervals
    // And we throw a die in the ring and decide which worker to pick
    // For example if worker1 has capacity 10, worker2 has 20, worker3 has 40,
    // the ring will look like [0, 10), [10, 30), [30, 70).
    // A key in the map is the LHS of a range.
    // So the map will look like {0 -> w1, 10 -> w2, 30 -> w3}.
    TreeMap<Long, BlockWorkerInfo> rangeStartMap = new TreeMap<>();
    AtomicLong totalCapacity = new AtomicLong(0L);
    sortedBlockWorkerList.forEach(workerInfo -> {
      if (workerInfo.getCapacityBytes() > 0) {
        long capacityRangeStart = totalCapacity.getAndAdd(workerInfo.getCapacityBytes());
        rangeStartMap.put(capacityRangeStart, workerInfo);
      }
    });
    if (totalCapacity.get() == 0L) {
      return Optional.empty();
    }
    long randomLong = randomInCapacity(options.getBlockInfo().getBlockId(), totalCapacity.get());
    WorkerNetAddress targetWorker = rangeStartMap.floorEntry(randomLong).getValue().getNetAddress();
    return Optional.of(targetWorker);
  }

  protected long randomInCapacity(Long blockId, long totalCapacity) {
    if (mMaxReplicaSize < 0) {
      return ThreadLocalRandom.current().nextLong(totalCapacity);
    }
    // blockId base hash value to decide which worker to cache data,
    // so the same block will be routed to the same set of worker.
    long sourceValue = blockId + ThreadLocalRandom.current().nextInt(mMaxReplicaSize);
    return Math.abs(MurmurHash3.hash64(sourceValue)) % totalCapacity;
  }

  private List<BlockWorkerInfo> toSortedList(Iterable<BlockWorkerInfo> blockWorkerInfos) {
    List<BlockWorkerInfo> blockWorkerInfoList = new ArrayList<>();
    blockWorkerInfos.forEach(blockWorkerInfoList::add);
    blockWorkerInfoList.sort(Comparator.comparing(a -> a.getNetAddress().getHost()));
    return blockWorkerInfoList;
  }
}
