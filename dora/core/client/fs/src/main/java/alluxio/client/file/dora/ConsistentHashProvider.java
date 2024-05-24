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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
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
public class ConsistentHashProvider {
  private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();
  private final int mMaxAttempts;
  private final long mWorkerInfoUpdateIntervalNs;

  /**
   * Timestamp of the last update to {@link #mActiveNodesByConsistentHashing}.
   * Must use System.nanoTime to ensure monotonic increment. Otherwise, earlier updates
   * may overwrite the latest as the expiry based on TTL cannot be reliably determined.
   */
  private final AtomicLong mLastUpdatedTimestamp = new AtomicLong(System.nanoTime());
  /**
   * Counter for how many times the map has been updated.
   */
  private final LongAdder mUpdateCount = new LongAdder();
  /**
   * The worker list which the {@link #mActiveNodesByConsistentHashing} was built from.
   * Must be kept in sync with {@link #mActiveNodesByConsistentHashing}.
   * Used to compare with incoming worker list to skip the heavy build process if the worker
   * list has not changed.
   */
  private final AtomicReference<Set<WorkerIdentity>> mLastWorkers =
      new AtomicReference<>(ImmutableSet.of());
  /**
   * Requirements for interacting with this map:
   * 1. This hash ring is lazy initialized (cannot init in the constructor).
   *    Multiple threads may try to enter the init section and it should be only initialized once.
   * 2. This hash ring is timestamped. After the TTL expires, we need to compare the worker
   *    list with the current available workers and possibly rebuild the hash ring.
   * 3. While the hash ring is being updated, readers should see a stale hash ring
   *    without blocking.
   * <p>
   * Thread safety guarantees:
   * 1. At lazy-init time, mutual exclusion is provided by `synchronized(mInitLock)`
   *    and double-checking. At this stage it is guarded by `mInitLock`.
   * 2. After init, updating the hash ring is guarded by an optimistic lock using CAS(timestamp).
   *    There will be no blocking but a read may see a stale ring.
   *    At this stage it is guarded by `mLastUpdatedTimestamp`.
   */
  @Nullable
  private volatile NavigableMap<Integer, WorkerIdentity> mActiveNodesByConsistentHashing;
  /**
   * Lock to protect the lazy initialization of {@link #mActiveNodesByConsistentHashing}.
   */
  private final Object mInitLock = new Object();

  /**
   * This is the number of virtual nodes in the consistent hashing algorithm.
   * In a consistent hashing algorithm, on membership changes, some virtual nodes are
   * re-distributed instead of rebuilding the whole hash table.
   * This guarantees the hash table is changed only in a minimal.
   * In order to achieve that, the number of virtual nodes should be X times the physical nodes
   * in the cluster, where X is a balance between redistribution granularity and size.
   */
  private final int mNumVirtualNodes;

  /**
   * Constructor.
   *
   * @param maxAttempts max attempts to rehash
   * @param workerListTtlMs interval between retries
   * @param numVirtualNodes number of virtual nodes
   */
  public ConsistentHashProvider(int maxAttempts, long workerListTtlMs, int numVirtualNodes) {
    mMaxAttempts = maxAttempts;
    mWorkerInfoUpdateIntervalNs = workerListTtlMs * Constants.MS_NANO;
    mNumVirtualNodes = numVirtualNodes;
  }

  /**
   * Finds multiple workers from the hash ring.
   *
   * @param key the key to hash on
   * @param count the expected number of workers
   * @return a list of workers following the hash ring
   */
  public List<WorkerIdentity> getMultiple(String key, int count) {
    Set<WorkerIdentity> workers = new LinkedHashSet<>(); // preserve insertion order
    int attempts = 0;
    while (workers.size() < count && attempts < mMaxAttempts) {
      attempts++;
      WorkerIdentity selectedWorker = get(key, attempts);
      workers.add(selectedWorker);
    }
    return ImmutableList.copyOf(workers);
  }

  /**
   * Initializes or refreshes the worker list using the given list of workers and number of
   * virtual nodes.
   * <br>
   * Thread safety:
   * If called concurrently by two or more threads, only one of the callers will actually
   * update the state of the hash provider using the worker list provided by that thread, and all
   * others will not change the internal state of the hash provider.
   *
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
        Set<WorkerIdentity> newWorkerIds = ImmutableSet.copyOf(workers);
        NavigableMap<Integer, WorkerIdentity> nodes = build(newWorkerIds, mNumVirtualNodes);
        mActiveNodesByConsistentHashing = nodes;
        mLastWorkers.set(newWorkerIds);
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
    if (mActiveNodesByConsistentHashing == null) {
      synchronized (mInitLock) {
        // only one thread should reach here
        // test again to skip re-initialization
        if (mActiveNodesByConsistentHashing == null) {
          Set<WorkerIdentity> workerIdentities = ImmutableSet.copyOf(workers);
          mActiveNodesByConsistentHashing = build(workerIdentities, mNumVirtualNodes);
          mLastWorkers.set(workerIdentities);
          mLastUpdatedTimestamp.set(System.nanoTime());
        }
      }
    }
  }

  @VisibleForTesting
  WorkerIdentity get(String key, int index) {
    NavigableMap<Integer, WorkerIdentity> map = mActiveNodesByConsistentHashing;
    Preconditions.checkState(map != null, "Hash provider is not properly initialized");
    return get(map, key, index);
  }

  @VisibleForTesting
  static WorkerIdentity get(NavigableMap<Integer, WorkerIdentity> map, String key, int index) {
    HashCode hashCode = HASH_FUNCTION.newHasher()
        .putString(key, UTF_8)
        .putInt(index)
        .hash();
    int hashKey = hashCode.asInt();
    Map.Entry<Integer, WorkerIdentity> entry = map.ceilingEntry(hashKey);
    if (entry != null) {
      return entry.getValue();
    } else {
      Map.Entry<Integer, WorkerIdentity> firstEntry = map.firstEntry();
      if (firstEntry == null) {
        throw new IllegalStateException("Hash provider is empty");
      }
      return firstEntry.getValue();
    }
  }

  @VisibleForTesting
  Set<WorkerIdentity> getLastWorkers() {
    return mLastWorkers.get();
  }

  @VisibleForTesting
  NavigableMap<Integer, WorkerIdentity> getActiveNodesMap() {
    return mActiveNodesByConsistentHashing;
  }

  @VisibleForTesting
  long getUpdateCount() {
    return mUpdateCount.sum();
  }

  @VisibleForTesting
  static NavigableMap<Integer, WorkerIdentity> build(
      Collection<WorkerIdentity> workers, int numVirtualNodes) {
    Preconditions.checkArgument(!workers.isEmpty(), "worker list is empty");
    NavigableMap<Integer, WorkerIdentity> activeNodesByConsistentHashing = new TreeMap<>();
    for (WorkerIdentity worker : workers) {
      for (int i = 0; i < numVirtualNodes; i++) {
        final HashCode hashCode = HASH_FUNCTION.newHasher()
            .putObject(worker, WorkerIdentity.HashFunnel.INSTANCE)
            .putInt(i)
            .hash();
        activeNodesByConsistentHashing.put(hashCode.asInt(), worker);
      }
    }
    return activeNodesByConsistentHashing;
  }
}
