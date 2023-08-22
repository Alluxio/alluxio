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
import static java.nio.charset.StandardCharsets.UTF_8;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A consistent hashing algorithm implementation.
 *
 * This implementation is thread safe in lazy init and in refreshing the worker list.
 * See inline comments for thread safety guarantees and semantics.
 */
@VisibleForTesting
@ThreadSafe
public class KetamaHashProvider {
  private final int mReplicas;
  private final int mMaxAttempts;
  private final long mWorkerInfoUpdateIntervalNs;
  private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();

  private final AtomicLong mLastUpdatedTimestamp = new AtomicLong(System.nanoTime());
  /**
   * Counter for how many times the map has been updated.
   */
  private final LongAdder mUpdateCount = new LongAdder();

  private final AtomicReference<List<BlockWorkerInfo>> mLastWorkerInfos =
      new AtomicReference<>(ImmutableList.of());

  @Nullable
  private volatile SortedMap<Integer, BlockWorkerInfo> mActiveNodes;
  /**
   * Lock to protect the lazy initialization of {@link #mActiveNodes}.
   */
  private final Object mInitLock = new Object();

  /**
   * Constructor.
   *
   * @param maxAttempts max attempts to rehash
   * @param workerListTtlMs interval between retries
   * @param replicas the number of replicas of workers
   */
  public KetamaHashProvider(int maxAttempts, long workerListTtlMs, int replicas) {
    mMaxAttempts = maxAttempts;
    mWorkerInfoUpdateIntervalNs = workerListTtlMs * Constants.MS_NANO;
    mReplicas = replicas;
  }

  /**
   * Finds multiple workers from the hash ring.
   *
   * @param key the key to hash on
   * @param count the expected number of workers
   * @return a list of workers following the hash ring
   */
  public List<BlockWorkerInfo> getMultiple(String key, int count) {
    Set<BlockWorkerInfo> workers = new HashSet<>();
    int attempts = 0;
    while (workers.size() < count && attempts < mMaxAttempts) {
      attempts++;
      workers.add(get(key, attempts));
    }
    return ImmutableList.copyOf(workers);
  }

  /**
   * Initializes or refreshes the worker list using the given list of workers.
   * @param workerInfos the up-to-date worker list
   */
  public void refresh(List<BlockWorkerInfo> workerInfos) {
    Preconditions.checkArgument(!workerInfos.isEmpty(),
        "cannot refresh hash provider with empty worker list");
    maybeInitialize(workerInfos);
    // check if the worker list has expired
    if (shouldRebuildActiveNodesMapExclusively()) {
      // thread safety is valid provided that build() takes less than
      // WORKER_INFO_UPDATE_INTERVAL_NS, so that before next update the current update has been
      // finished
      if (hasWorkerListChanged(workerInfos, mLastWorkerInfos.get())) {
        updateActiveNodes(workerInfos, mLastWorkerInfos.get());
        mLastWorkerInfos.set(workerInfos);
        mUpdateCount.increment();
      }
    }
    // otherwise, do nothing and proceed with stale worker list. on next access, the worker list
    // will have been updated by another thread
  }

  /**
   * Check whether the current map has expired and needs update.
   * If called by multiple threads concurrently, only one of the callers will get a return value
   * of true, so that the map will be updated only once. The other threads will not try to
   * update and use stale information instead.
   */
  private boolean shouldRebuildActiveNodesMapExclusively() {
    // check if the worker list has expired
    long lastUpdateTs = mLastUpdatedTimestamp.get();
    long currentTs = System.nanoTime();
    if (currentTs - lastUpdateTs > mWorkerInfoUpdateIntervalNs) {
      // use CAS to only allow one thread to actually update the timestamp
      return mLastUpdatedTimestamp.compareAndSet(lastUpdateTs, currentTs);
    }
    return false;
  }

  /**
   * Lazily initializes the hash ring.
   * Only one caller gets to initialize the map while all others are blocked.
   * After the initialization, the map must not be null.
   */
  private void maybeInitialize(List<BlockWorkerInfo> workerInfos) {
    if (mActiveNodes == null) {
      synchronized (mInitLock) {
        // only one thread should reach here
        // test again to skip re-initialization
        if (mActiveNodes == null) {
          build(workerInfos);
          mLastWorkerInfos.set(workerInfos);
          mLastUpdatedTimestamp.set(System.nanoTime());
        }
      }
    }
  }

  /**
   * Whether the worker list has changed.
   * @param workerInfoList
   * @param anotherWorkerInfoList
   * @return
   */
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

  /**
   * Update the active nodes.
   * @param workerInfos
   * @param lastWorkerInfos
   */
  private void updateActiveNodes(List<BlockWorkerInfo> workerInfos,
                                 List<BlockWorkerInfo> lastWorkerInfos) {
    HashSet<BlockWorkerInfo> workerInfoSet = new HashSet<>(workerInfos);
    HashSet<BlockWorkerInfo> lastWorkerInfoSet = new HashSet<>(lastWorkerInfos);
    // remove the workers that are no longer active
    for (BlockWorkerInfo workerInfo : lastWorkerInfoSet) {
      if (!workerInfoSet.contains(workerInfo)) {
        remove(workerInfo);
      }
    }
    // add the new workers
    for (BlockWorkerInfo workerInfo : workerInfoSet) {
      if (!lastWorkerInfoSet.contains(workerInfo)) {
        add(workerInfo);
      }
    }
  }

  @VisibleForTesting
  BlockWorkerInfo get(String key, int index) {
    Preconditions.checkState(mActiveNodes != null, "Hash provider is not properly initialized");
    if (mActiveNodes.isEmpty()) {
      return null;
    }
    int hash = hash(String.format("%s%d", key, index));
    if (!mActiveNodes.containsKey(hash)) {
      SortedMap<Integer, BlockWorkerInfo> tailMap = mActiveNodes.tailMap(hash);
      hash = tailMap.isEmpty() ? mActiveNodes.firstKey() : tailMap.firstKey();
    }
    return mActiveNodes.get(hash);
  }

  @VisibleForTesting
  List<BlockWorkerInfo> getLastWorkerInfos() {
    return mLastWorkerInfos.get();
  }

  @VisibleForTesting
  SortedMap<Integer, BlockWorkerInfo> getActiveNodesMap() {
    return mActiveNodes;
  }

  @VisibleForTesting
  long getUpdateCount() {
    return mUpdateCount.sum();
  }

  @VisibleForTesting
  private void build(
      List<BlockWorkerInfo> workerInfos) {
    Preconditions.checkArgument(!workerInfos.isEmpty(), "worker list is empty");
    mActiveNodes = new TreeMap<>();
    for (BlockWorkerInfo workerInfo : workerInfos) {
      add(workerInfo);
    }
  }

  private void add(BlockWorkerInfo node) {
    Preconditions.checkState(mActiveNodes != null, "Hash provider is not properly initialized");
    String nodeInfo = node.getNetAddress().dumpMainInfo();
    for (int i = 0; i < mReplicas; i++) {
      mActiveNodes.put(hash(i + nodeInfo + i), node);
    }
  }

  private void remove(BlockWorkerInfo node) {
    Preconditions.checkState(mActiveNodes != null, "Hash provider is not properly initialized");
    String nodeInfo = node.getNetAddress().dumpMainInfo();
    for (int i = 0; i < mReplicas; i++) {
      mActiveNodes.remove(hash(i + nodeInfo + i));
    }
  }

  private int hash(String key) {
    return HASH_FUNCTION.hashString(key, UTF_8).asInt();
  }
}
