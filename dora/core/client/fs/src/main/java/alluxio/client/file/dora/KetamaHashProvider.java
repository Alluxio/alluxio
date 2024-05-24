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
import alluxio.wire.WorkerIdentity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
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

  private final AtomicReference<Set<WorkerIdentity>> mLastWorkers =
      new AtomicReference<>(ImmutableSet.of());

  @Nullable
  private volatile SortedMap<Integer, WorkerIdentity> mActiveNodes;
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
  public List<WorkerIdentity> getMultiple(String key, int count) {
    Set<WorkerIdentity> workers = new LinkedHashSet<>();
    int attempts = 0;
    while (workers.size() < count && attempts < mMaxAttempts) {
      attempts++;
      WorkerIdentity selectedWorker = get(key, attempts);
      workers.add(selectedWorker);
    }
    return ImmutableList.copyOf(workers);
  }

  /**
   * Initializes or refreshes the worker list using the given list of workers.
   * @param workers the up-to-date worker list
   */
  public void refresh(Set<WorkerIdentity> workers) {
    Preconditions.checkArgument(!workers.isEmpty(),
        "cannot refresh hash provider with empty worker list");
    maybeInitialize(workers);
    // check if the worker list has expired
    if (shouldRebuildActiveNodesMapExclusively()) {
      // thread safety is valid provided that build() takes less than
      // WORKER_INFO_UPDATE_INTERVAL_NS, so that before next update the current update has been
      // finished
      Set<WorkerIdentity> lastWorkerIds = mLastWorkers.get();
      if (!workers.equals(lastWorkerIds)) {
        updateActiveNodes(workers, mLastWorkers.get());
        mLastWorkers.set(workers);
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
  private void maybeInitialize(Set<WorkerIdentity> workers) {
    if (mActiveNodes == null) {
      synchronized (mInitLock) {
        // only one thread should reach here
        // test again to skip re-initialization
        if (mActiveNodes == null) {
          build(workers);
          mLastWorkers.set(workers);
          mLastUpdatedTimestamp.set(System.nanoTime());
        }
      }
    }
  }

  /**
   * Update the active nodes.
   * @param workers
   * @param lastWorkers
   */
  private void updateActiveNodes(Set<WorkerIdentity> workers,
                                 Set<WorkerIdentity> lastWorkers) {
    HashSet<WorkerIdentity> workerSet = new HashSet<>(workers);
    HashSet<WorkerIdentity> lastWorkerSet = new HashSet<>(lastWorkers);
    // remove the workers that are no longer active
    for (WorkerIdentity worker : lastWorkerSet) {
      if (!workerSet.contains(worker)) {
        remove(worker);
      }
    }
    // add the new workers
    for (WorkerIdentity worker : workerSet) {
      if (!lastWorkerSet.contains(worker)) {
        add(worker);
      }
    }
  }

  @VisibleForTesting
  WorkerIdentity get(String key, int index) {
    Preconditions.checkState(mActiveNodes != null, "Hash provider is not properly initialized");
    if (mActiveNodes.isEmpty()) {
      return null;
    }
    int hash = hash(String.format("%s%d", key, index));
    if (!mActiveNodes.containsKey(hash)) {
      SortedMap<Integer, WorkerIdentity> tailMap = mActiveNodes.tailMap(hash);
      hash = tailMap.isEmpty() ? mActiveNodes.firstKey() : tailMap.firstKey();
    }
    return mActiveNodes.get(hash);
  }

  @VisibleForTesting
  Set<WorkerIdentity> getLastWorkers() {
    return mLastWorkers.get();
  }

  @VisibleForTesting
  SortedMap<Integer, WorkerIdentity> getActiveNodesMap() {
    return mActiveNodes;
  }

  @VisibleForTesting
  long getUpdateCount() {
    return mUpdateCount.sum();
  }

  @VisibleForTesting
  private void build(
      Set<WorkerIdentity> workers) {
    Preconditions.checkArgument(!workers.isEmpty(), "worker list is empty");
    mActiveNodes = new TreeMap<>();
    for (WorkerIdentity worker : workers) {
      add(worker);
    }
  }

  private void add(WorkerIdentity node) {
    Preconditions.checkState(mActiveNodes != null, "Hash provider is not properly initialized");
    final HashCode hashCode = HASH_FUNCTION.newHasher()
        .putObject(node, WorkerIdentity.HashFunnel.INSTANCE).hash();
    for (int i = 0; i < mReplicas; i++) {
      mActiveNodes.put(hashCode.asInt() + i, node);
    }
  }

  private void remove(WorkerIdentity node) {
    Preconditions.checkState(mActiveNodes != null, "Hash provider is not properly initialized");
    final HashCode hashCode = HASH_FUNCTION.newHasher()
        .putObject(node, WorkerIdentity.HashFunnel.INSTANCE).hash();
    for (int i = 0; i < mReplicas; i++) {
      mActiveNodes.remove(hashCode.asInt() + i);
    }
  }

  private int hash(String key) {
    return HASH_FUNCTION.hashString(key, UTF_8).asInt();
  }
}
