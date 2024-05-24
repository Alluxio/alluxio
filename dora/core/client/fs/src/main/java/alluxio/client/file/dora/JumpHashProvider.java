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
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import alluxio.Constants;
import alluxio.wire.WorkerIdentity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nullable;

/**
 * An impl of JumpConsistentHash.
 */
public class JumpHashProvider {
  private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();
  private final int mMaxAttempts;
  private final long mWorkerInfoUpdateIntervalNs;
  private static final long UNSIGNED_MASK = 0x7fffffffffffffffL;

  private static final long JUMP = 1L << 31;

  private static final long CONSTANT = Long
      .parseUnsignedLong("2862933555777941757");

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
   * 1. This bucket list is lazy initialized (cannot init in the constructor).
   *    Multiple threads may try to enter the init section, and it should be only initialized once.
   * 2. This bucket list is timestamped. After the TTL expires, we need to compare the worker
   *    list with the current available workers and possibly rebuild the bucket list.
   * 3. While the bucket list is being updated, readers should see a stale bucket list
   *    without blocking.
   *
   * Thread safety guarantees:
   * 1. At lazy-init time, mutual exclusion is provided by `synchronized(mInitLock)`
   *    and double-checking. At this stage it is guarded by `mInitLock`.
   * 2. After init, updating the bucket list is guarded by an optimistic lock using CAS(timestamp).
   *    There will be no blocking but a read may see a stale bucket list.
   *    At this stage it is guarded by `mLastUpdatedTimestamp`.
   */
  @Nullable
  private volatile HashMap<Integer, WorkerIdentity> mActiveNodesByConsistentHashing;
  /**
   * Lock to protect the lazy initialization of {@link #mActiveNodesByConsistentHashing}.
   */
  private final Object mInitLock = new Object();

  /**
   * @param maxAttempts the max attempts
   * @param workerListTtlMs interval between retries
   */
  public JumpHashProvider(int maxAttempts, long workerListTtlMs) {
    mMaxAttempts = maxAttempts;
    mWorkerInfoUpdateIntervalNs = workerListTtlMs * Constants.MS_NANO;
  }

  /**
   * Finds multiple workers from the buckets.
   *
   * @param key the key to use for hashing
   * @param count the expected number of workers
   * @return a list of workers following the worker list
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
   * <br>
   * Thread safety:
   * If called concurrently by two or more threads, only one of the callers will actually
   * update the state of the hash provider using the worker list provided by that thread, and all
   * others will not change the internal state of the hash provider.
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
        mActiveNodesByConsistentHashing = build(workers);
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
   * Lazily initializes the bucket list.
   * Only one caller gets to initialize the map while all others are blocked.
   * After the initialization, the map must not be null.
   */
  private void maybeInitialize(Set<WorkerIdentity> workers) {
    if (mActiveNodesByConsistentHashing == null) {
      synchronized (mInitLock) {
        // only one thread should reach here
        // test again to skip re-initialization
        if (mActiveNodesByConsistentHashing == null) {
          mActiveNodesByConsistentHashing = build(workers);
          mLastWorkers.set(workers);
          mLastUpdatedTimestamp.set(System.nanoTime());
        }
      }
    }
  }

  WorkerIdentity get(String key, int index) {
    HashMap<Integer, WorkerIdentity> hashMap = mActiveNodesByConsistentHashing;
    Preconditions.checkState(hashMap != null, "Hash provider is not properly initialized");
    return get(hashMap, key, index);
  }

  @VisibleForTesting
  static WorkerIdentity get(HashMap<Integer, WorkerIdentity> hashMap, String key, int index) {
    int hashKey = HASH_FUNCTION.hashString(format("%s%d", key, index), UTF_8).asInt();
    int workerId = jumpConsistentHash(hashKey, hashMap.size());
    return hashMap.get(workerId);
  }

  /**
   * Accepts "a 64-bit key and the number of buckets. It outputs a number in
   * the range [0, buckets]."
   *
   * @param key
   *            key to store
   * @param buckets
   *            number of available buckets
   * @return the hash of the key
   */
  private static int jumpConsistentHash(final int key, final int buckets) {
    long hashValue = -1;
    long k = key;
    long j = 0;
    while (j < buckets) {
      hashValue = j;
      k = k * CONSTANT + 1L;
      j = (long) ((hashValue + 1L) * (JUMP / toDouble((k >>> 33) + 1L)));
    }
    return (int) hashValue;
  }

  private static double toDouble(final long n) {
    double d = n & UNSIGNED_MASK;
    if (n < 0) {
      d += 0x1.0p63;
    }
    return d;
  }

  @VisibleForTesting
  Set<WorkerIdentity> getLastWorkers() {
    return mLastWorkers.get();
  }

  @VisibleForTesting
  HashMap<Integer, WorkerIdentity> getActiveNodesMap() {
    return mActiveNodesByConsistentHashing;
  }

  @VisibleForTesting
  long getUpdateCount() {
    return mUpdateCount.sum();
  }

  @VisibleForTesting
  static HashMap<Integer, WorkerIdentity> build(
      Set<WorkerIdentity> workers) {
    Preconditions.checkArgument(!workers.isEmpty(), "worker list is empty");
    HashMap<Integer, WorkerIdentity> activeNodesByJumpConsistentHashing = new HashMap<>();
    int workerIndex = 0;
    for (WorkerIdentity worker : workers) {
      activeNodesByJumpConsistentHashing.put(workerIndex, worker);
      workerIndex++;
    }
    return activeNodesByJumpConsistentHashing;
  }
}
