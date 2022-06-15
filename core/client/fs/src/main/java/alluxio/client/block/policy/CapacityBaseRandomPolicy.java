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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Randomly distribute workload based on the worker capacities so bigger workers get more requests.
 * The randomness is based on the capacity instead of availability because in the long run,
 * all workers will be filled up and have availability close to 0.
 * We do not want the policy to degenerate to all workers having the same chance.
 */
@ThreadSafe
public class CapacityBaseRandomPolicy implements BlockLocationPolicy {
  private final Cache<Long, List<WorkerNetAddress>> mBlockLocationCache;

  private final int mMaxReplicaSize;

  /**
   * Constructs a new {@link CapacityBaseRandomPolicy}
   * needed for instantiation in {@link BlockLocationPolicy.Factory}.
   *
   * @param conf Alluxio configuration
   */
  public CapacityBaseRandomPolicy(AlluxioConfiguration conf) {
    Duration expirationTime =
        conf.getDuration(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_CACHE_EXPIRATION_TIME);
    int cacheSize = conf.getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_CACHE_SIZE);
    mBlockLocationCache =
        CacheBuilder.newBuilder().maximumSize(cacheSize).expireAfterWrite(expirationTime).build();
    mMaxReplicaSize = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MAX);
  }

  @Override
  public Optional<WorkerNetAddress> getWorker(GetWorkerOptions options) {
    WorkerNetAddress cacheAddress = findCacheWorker(options);
    if (cacheAddress != null) {
      return Optional.of(cacheAddress);
    }

    Iterable<BlockWorkerInfo> blockWorkerInfos = options.getBlockWorkerInfos();

    // All the capacities will form a ring of continuous intervals
    // And we throw a die in the ring and decide which worker to pick
    // For example if worker1 has capacity 10, worker2 has 20, worker3 has 40,
    // the ring will look like [0, 10), [10, 30), [30, 70).
    // A key in the map is the LHS of a range.
    // So the map will look like {0 -> w1, 10 -> w2, 30 -> w3}.
    TreeMap<Long, BlockWorkerInfo> rangeStartMap = new TreeMap<>();
    AtomicLong totalCapacity = new AtomicLong(0L);
    blockWorkerInfos.forEach(workerInfo -> {
      if (workerInfo.getCapacityBytes() > 0) {
        long capacityRangeStart = totalCapacity.getAndAdd(workerInfo.getCapacityBytes());
        rangeStartMap.put(capacityRangeStart, workerInfo);
      }
    });
    if (totalCapacity.get() == 0L) {
      return Optional.empty();
    }
    long randomLong = randomInCapacity(totalCapacity.get());
    WorkerNetAddress targetWorker = rangeStartMap.floorEntry(randomLong).getValue().getNetAddress();
    addWorkerToCache(options.getBlockInfo().getBlockId(), targetWorker);
    return Optional.of(targetWorker);
  }

  protected long randomInCapacity(long totalCapacity) {
    return ThreadLocalRandom.current().nextLong(totalCapacity);
  }

  protected WorkerNetAddress findCacheWorker(GetWorkerOptions options) {
    List<WorkerNetAddress> cacheCandidateList =
        mBlockLocationCache.getIfPresent(options.getBlockInfo().getBlockId());
    if (cacheCandidateList != null && mMaxReplicaSize > 0) {
      Set<WorkerNetAddress> eligibleAddresses = new HashSet<>();
      for (BlockWorkerInfo info : options.getBlockWorkerInfos()) {
        eligibleAddresses.add(info.getNetAddress());
      }
      List<WorkerNetAddress> eligibleCacheList =
          cacheCandidateList.stream().filter(eligibleAddresses::contains)
              .collect(Collectors.toList());
      if (eligibleCacheList.size() >= mMaxReplicaSize) {
        int index = ThreadLocalRandom.current().nextInt(eligibleCacheList.size());
        return eligibleCacheList.get(index);
      }
    }
    return null;
  }

  protected void addWorkerToCache(Long blockId, WorkerNetAddress targetWorker) {
    if (mMaxReplicaSize <= 0) {
      return;
    }
    List<WorkerNetAddress> cacheWorkers = mBlockLocationCache.getIfPresent(blockId);
    if (cacheWorkers == null) {
      cacheWorkers = new CopyOnWriteArrayList<>();
      // guava cache is thread-safe
      mBlockLocationCache.put(blockId, cacheWorkers);
    }
    cacheWorkers.add(targetWorker);
  }
}
