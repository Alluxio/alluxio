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
import org.apache.curator.shaded.com.google.common.hash.Hashing;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A consistent hashing algorithm implementation.
 *
 * This implementation is thread safe in lazy init and in refreshing the worker list.
 * See inline comments for thread safety guarantees and semantics.
 */
@VisibleForTesting
@ThreadSafe
public class MaglevHashProvider {
  private final int mMaxAttempts;
  private final long mWorkerInfoUpdateIntervalNs;
  private static final HashFunction HASH_FUNCTION = murmur3_32_fixed();

  /**
   * Must use System.nanoTime to ensure monotonic increment. Otherwise, earlier updates
   * may overwrite the latest as the expiry based on TTL cannot be reliably determined.
   */
  private final AtomicLong mLastUpdatedTimestamp = new AtomicLong(System.nanoTime());

  /**
   * Counter for how many times the map has been updated.
   */
  private final LongAdder mUpdateCount = new LongAdder();

  private final AtomicReference<Set<WorkerIdentity>> mLastWorkers =
      new AtomicReference<>(ImmutableSet.of());

  /**
   * Lock to protect the lazy initialization of {@link #mLookup}.
   */
  private final Object mInitLock = new Object();

  /** Seed used to compute the lookup index. */
  private static final int INDEX_SEED = 0xDEADBEEF;

  /**
   * The lookup table size should be a prime number and should be much bigger
   * than the number of nodes (lookupSize >> maxNodes).
   */
  private final int mLookupSize;

  /**
   * The lookup table.
   */
  private WorkerIdentity[] mLookup;

  /** Maps each backend to the related permutation. */
  private Map<WorkerIdentity, Permutation> mPermutations;

  /**
   * Constructor.
   *
   * @param maxAttempts max attempts to rehash
   * @param workerListTtlMs interval between retries
   * @param lookupSize the size of the lookup table
   */
  public MaglevHashProvider(int maxAttempts, long workerListTtlMs, int lookupSize) {
    mMaxAttempts = maxAttempts;
    mWorkerInfoUpdateIntervalNs = workerListTtlMs * Constants.MS_NANO;
    mLookupSize = lookupSize;
    mPermutations = new HashMap<>();
  }

  /**
   * Finds multiple workers from the lookup table.
   *
   * @param key the key to hash on
   * @param count the expected number of workers
   * @return a list of workers to be mapped
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
   * Lazily initializes the hash map.
   * Only one caller gets to initialize the map while all others are blocked.
   * After the initialization, the map must not be null.
   */
  private void maybeInitialize(Set<WorkerIdentity> workers) {
    if (mLookup == null) {
      synchronized (mInitLock) {
        // only one thread should reach here
        // test again to skip re-initialization
        if (mLookup == null) {
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
    HashSet<WorkerIdentity> toRemove = new HashSet<>();
    HashSet<WorkerIdentity> toAdd = new HashSet<>();
    // remove the workers that are no longer active
    for (WorkerIdentity worker : lastWorkerSet) {
      if (!workerSet.contains(worker)) {
        toRemove.add(worker);
      }
    }
    // add the new workers
    for (WorkerIdentity worker : workerSet) {
      if (!lastWorkerSet.contains(worker)) {
        toAdd.add(worker);
      }
    }
    // remove the workers that are no longer active
    remove(toRemove);
    // add the new workers
    add(toAdd);
  }

  @VisibleForTesting
  WorkerIdentity get(String key, int index) {
    Preconditions.checkState(mLookup != null, "Hash provider is not properly initialized");
    if (mLookup.length == 0) {
      return null;
    }
    final int id = Math.abs(hash(String.format("%s%d%d", key, index, INDEX_SEED)) % mLookup.length);
    return mLookup[id];
  }

  @VisibleForTesting
  long getUpdateCount() {
    return mUpdateCount.sum();
  }

  @VisibleForTesting
  private void build(
      Set<WorkerIdentity> workers) {
    Preconditions.checkArgument(!workers.isEmpty(), "worker list is empty");
    mLookup = new WorkerIdentity[0];
    add(workers);
  }

  private void add(Set<WorkerIdentity> toAdd) {
    mPermutations.values().forEach(Permutation::reset);
    for (WorkerIdentity backend : toAdd) {
      mPermutations.put(backend, newPermutation(backend));
    }
    mLookup = newLookup();
  }

  private void remove(Collection<WorkerIdentity> toRemove) {
    toRemove.forEach(mPermutations::remove);
    mPermutations.values().forEach(Permutation::reset);
    mLookup = newLookup();
  }

  int hash(String key) {
    return HASH_FUNCTION.hashString(key, UTF_8).asInt();
  }

  /**
   * Creates a new permutation for the given backend.
   *
   * @param backend the source of the permutation
   * @return a new permutation
   */
  private Permutation newPermutation(WorkerIdentity backend) {
    return new Permutation(backend, mLookupSize);
  }

  /**
   * Creates a new lookup table.
   *
   * @return the new lookup table
   */
  private WorkerIdentity[] newLookup() {
    final WorkerIdentity[] lookup = new WorkerIdentity[mLookupSize];
    final AtomicInteger filled = new AtomicInteger();
    do {
      mPermutations.values().forEach(permutation -> {
        final int pos = permutation.next();
        if (lookup[pos] == null) {
          lookup[pos] = permutation.backend();
        }
      });
    } while (filled.incrementAndGet() < mLookupSize);
    return lookup;
  }

  class Permutation {
    /**
     * Seed used to compute the state offset.
     */
    private static final int OFFSET_SEED = 0xDEADBABE;

    /**
     * Seed used to compute the state skip.
     */
    private static final int SKIP_SEED = 0xDEADDEAD;

    /**
     * The backend associated to the permutation.
     */
    private final WorkerIdentity mBackend;

    /**
     * The size of the lookup table.
     */
    private final int mSize;

    /**
     * Position where to start.
     */
    private final int mOffset;

    /**
     * Positions to skip.
     */
    private final int mSkip;

    /**
     * The current value of the permutation.
     */
    private int mCurrent;

    int hash1(String key) {
      return HASH_FUNCTION.hashString(key, UTF_8).asInt();
    }

    int hash2(String key) {
      // use XXHash
      return Hashing.crc32c().hashString(key, UTF_8).asInt();
    }

    /**
     * Constructor with parameters.
     *
     * @param backend the backend to wrap
     * @param size    size of the lookup table
     */
    Permutation(WorkerIdentity backend, int size) {
      mSize = size;
      mBackend = backend;
      final HashCode hashCode = HASH_FUNCTION.newHasher()
          .putObject(backend, WorkerIdentity.HashFunnel.INSTANCE).hash();
      mOffset = hash1(String.format("%d%d",
          hashCode.asInt(), OFFSET_SEED)) % size;
      mSkip = hash2(String.format("%d%d",
          hashCode.asInt(), SKIP_SEED)) % (size - 1) + 1;
      mCurrent = mOffset;
    }

    /**
     * Returns the backend related to the current permutation.
     *
     * @return the backend related to the current permutation
     */
    WorkerIdentity backend() {
      return mBackend;
    }

    /**
     * Returns the next value in the permutation.
     *
     * @return the next value
     */
    int next() {
      mCurrent = (mCurrent + mSkip) % mSize;
      return Math.abs(mCurrent);
    }

    /**
     * Resets the permutation for the new lookup size.
     */
    void reset() {
      mCurrent = mOffset;
    }
  }
}
