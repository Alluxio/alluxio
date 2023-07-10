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

package alluxio.client.file.dora;

import static com.google.common.hash.Hashing.murmur3_32_fixed;
import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * An impl of WorkerLocationPolicy.
 */
public class WorkerLocationPolicy {
  private static final ConsistentHashProvider HASH_PROVIDER = new ConsistentHashProvider();
  private final int mNumVirtualNodes;

  /**
   * Constructs a new {@link WorkerLocationPolicy}.
   *
   * @param numVirtualNodes number of virtual nodes
   */
  public WorkerLocationPolicy(int numVirtualNodes) {
    mNumVirtualNodes = numVirtualNodes;
  }

  /**
   *
   * @param blockWorkerInfos
   * @param fileId
   * @param count
   * @return a list of preferred workers
   */
  public List<BlockWorkerInfo> getPreferredWorkers(List<BlockWorkerInfo> blockWorkerInfos,
                                                   String fileId,
                                                   int count) {
    if (blockWorkerInfos.size() == 0) {
      return ImmutableList.of();
    }
    HASH_PROVIDER.refresh(blockWorkerInfos, mNumVirtualNodes);
    return HASH_PROVIDER.getMultiple(fileId, count);
  }

  private static class ConsistentHashProvider {
    private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();
    private static final int MAX_ATTEMPTS = 100;
    private static final long WORKER_INFO_UPDATE_INTERVAL_NS = Constants.SECOND_NANO;
    private volatile List<BlockWorkerInfo> mLastWorkerInfos = ImmutableList.of();
    // Can only be null before the first call to refresh
    @Nullable
    private volatile NavigableMap<Integer, BlockWorkerInfo> mActiveNodesByConsistentHashing;
    private final Semaphore mInitLock = new Semaphore(1);

    // Must use System.nanoTime to ensure monotonic increment
    private final AtomicLong mLastUpdatedTimestamp = new AtomicLong(System.nanoTime());

    /**
     * Initializes or refreshes the worker list using the given list of workers and number of
     * virtual nodes.
     */
    public void refresh(List<BlockWorkerInfo> workerInfos, int numVirtualNodes) {
      // When the active nodes map does not exist, the hash provider is not initialized yet.
      // let one caller initialize the map while blocking all others.
      if (mActiveNodesByConsistentHashing == null) {
        mInitLock.acquireUninterruptibly();
        // only one thread should reach here
        // test again to skip re-initialization
        if (mActiveNodesByConsistentHashing == null) {
          try {
            mActiveNodesByConsistentHashing = build(workerInfos, numVirtualNodes);
          } finally {
            mLastWorkerInfos = workerInfos;
            mLastUpdatedTimestamp.set(System.nanoTime());
            mInitLock.release();
          }
        }
      }
      // check if the worker list has expired
      long lastUpdateTs = mLastUpdatedTimestamp.get();
      long currentTs = System.nanoTime();
      if (currentTs - lastUpdateTs > WORKER_INFO_UPDATE_INTERVAL_NS) {
        // use CAS to only allow one thread to actually update the timestamp
        boolean casUpdated = mLastUpdatedTimestamp.compareAndSet(lastUpdateTs, currentTs);
        // thread safety is valid provided that build() takes less than
        // WORKER_INFO_UPDATE_INTERVAL_NS, so that before next update the current update has been
        // finished
        if (casUpdated) {
          if (hasWorkerListChanged(workerInfos, mLastWorkerInfos)) {
            try {
              mActiveNodesByConsistentHashing = build(workerInfos, numVirtualNodes);
            } finally {
              mLastWorkerInfos = workerInfos;
            }
          }
        }
        // else, do nothing and proceed with stale worker list. on next access, the worker list
        // will have been updated by another thread
      }
    }

    private boolean hasWorkerListChanged(List<BlockWorkerInfo> workerInfoList,
                                         List<BlockWorkerInfo> anotherWorkerInfoList) {
      if (workerInfoList == anotherWorkerInfoList) {
        return false;
      }
      Set<WorkerNetAddress> workerAddressSet = workerInfoList.stream()
          .map(info -> info.getNetAddress()).collect(Collectors.toSet());
      Set<WorkerNetAddress> anotherWorkerAddressSet = anotherWorkerInfoList.stream()
          .map(info -> info.getNetAddress()).collect(Collectors.toSet());
      return !workerAddressSet.equals(anotherWorkerAddressSet);
    }

    public List<BlockWorkerInfo> getMultiple(String key, int count) {
      Set<BlockWorkerInfo> workers = new HashSet<>();
      int attempts = 0;
      while (workers.size() < count && attempts < MAX_ATTEMPTS) {
        attempts++;
        workers.add(get(key, attempts));
      }
      return ImmutableList.copyOf(workers);
    }

    public BlockWorkerInfo get(String key, int index) {
      int hashKey = HASH_FUNCTION.hashString(format("%s%d", key, index), UTF_8).asInt();
      NavigableMap<Integer, BlockWorkerInfo> map = mActiveNodesByConsistentHashing;
      if (map == null) {
        throw new IllegalStateException("Hash provider is not properly initialized");
      }
      Map.Entry<Integer, BlockWorkerInfo> entry = map.ceilingEntry(hashKey);
      if (entry != null) {
        return entry.getValue();
      } else {
        Map.Entry<Integer, BlockWorkerInfo> firstEntry = map.firstEntry();
        if (firstEntry == null) {
          throw new IllegalStateException("Hash provider is empty");
        }
        return firstEntry.getValue();
      }
    }

    private static NavigableMap<Integer, BlockWorkerInfo> build(
        List<BlockWorkerInfo> workerInfos, int numVirtualNodes) {
      NavigableMap<Integer, BlockWorkerInfo> activeNodesByConsistentHashing = new TreeMap<>();
      int weight = (int) ceil(1.0 * numVirtualNodes / workerInfos.size());
      for (BlockWorkerInfo workerInfo : workerInfos) {
        for (int i = 0; i < weight; i++) {
          activeNodesByConsistentHashing.put(
              HASH_FUNCTION.hashString(format("%s%d", workerInfo.getNetAddress().dumpMainInfo(), i),
                  UTF_8).asInt(),
              workerInfo);
        }
      }
      return activeNodesByConsistentHashing;
    }
  }
}
