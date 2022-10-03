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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;
import alluxio.resource.LockResource;
import alluxio.util.io.PathUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A cache of paths that have been synchronized or have been marked as not being synchronized with
 * the UFS. Paths are mapped to time values that indicate their last synchronization or
 * invalidation.
 *
 * Synchronization times are updated only at the root path for which a sync was performed by
 * {@link alluxio.master.file.InodeSyncStream}.
 * Each path contains the following values concerning synchronization:
 * - recursiveValidationTime: the last time a sync was successful on this path that included syncing
 * all of its children recursively
 * - validationTime: the last time a sync was successful on this path
 * - directValidationTime: the last time a sync was successful on the path which (at least)
 * included itself and its direct children
 * - recursiveValidationTime: the last time a sync was successful on this path which (at least)
 * included itself and all its children
 * Each path additionally contains the following values considering invalidation:
 * - invalidationTime: the last time this exact path was invalidated
 * - directChildInvalidation: the last time a direct child of this path was invalidated
 * - recursiveChildInvalidation: the last time a non-direct child of this path was invalidated
 * - isFile: when checking if a file needs to be synced, any descendant type from the most
 * recent sync will be valid
 *
 * Whenever an invalidation is received the path, and its parents up to the root have their
 * appropriate invalidation times updated. Validation times are updated on the path
 * when {@link #notifySyncedPath} is called on the root sync path after a successful sync.
 * An invalidation is received either when a client calls
 * {@link alluxio.master.file.DefaultFileSystemMaster#invalidateSyncPath} to notify that a path
 * needs synchronization, or when a file is updated by an external Alluxio cluster, and
 * cross cluster sync is enabled.
 *
 * Checking if a path needs to be synchronized involves checking the appropriate validation
 * and invalidation times at each path component up to the root.
 */
@ThreadSafe
public class InvalidationSyncCache {
  private static final Logger LOG = LoggerFactory.getLogger(InvalidationSyncCache.class);

  public final Cache<String, SyncState> mItems;
  private final Clock mClock;
  private final Function<AlluxioURI, Optional<AlluxioURI>> mReverseResolution;
  // we keep the root as a separate value as we never want to evict it,
  // and instead we just update it under a lock
  private final SyncState mRoot = new SyncState(false);
  private final Lock mRootLock = new ReentrantLock();

  /**
   * Creates a new instance of {@link InvalidationSyncCache}.
   * @param clock the clock to use to compute sync times
   * @param reverseResolution function from ufs path to alluxio path
   */
  public InvalidationSyncCache(Clock clock,
      Function<AlluxioURI, Optional<AlluxioURI>> reverseResolution) {
    this(clock, reverseResolution, null);
  }

  @VisibleForTesting
  InvalidationSyncCache(Clock clock, Function<AlluxioURI, Optional<AlluxioURI>> reverseResolution,
      @Nullable BiConsumer<String, SyncState> onRemoval) {
    mReverseResolution = Objects.requireNonNull(reverseResolution);
    mClock = Objects.requireNonNull(clock);
    mItems = CacheBuilder.newBuilder()
        .removalListener(
            (removal) -> {
              if (removal.wasEvicted() && removal.getKey() != null && removal.getValue() != null) {
                if (onRemoval != null) {
                  onRemoval.accept((String) removal.getKey(), (SyncState) removal.getValue());
                }
                onCacheEviction((String) removal.getKey(), (SyncState) removal.getValue());
              }
            })
        .maximumSize(Configuration.getInt(
            PropertyKey.MASTER_UFS_PATH_CACHE_CAPACITY))
        .build();
  }

  /**
   * Called when an element is evicted from the cache.
   * @param path the path
   * @param state the state
   */
  void onCacheEviction(String path, SyncState state) {
    try {
      // on eviction, we must mark our parent as needing a sync with our invalidation time
      notifyInvalidationInternal(PathUtils.getParentCleaned(path), state.mInvalidationTime);
    } catch (InvalidPathException e) {
      throw new RuntimeException("Should not have an invalid path in the cache", e);
    }
  }

  /**
   * There is a most one writer at a time to a sync state.
   * But multiple readers may be reading the values while they
   * are being updated, thus the values are volatile.
   */
  static class SyncState {
    volatile long mDirectChildInvalidation = 0;
    volatile long mRecursiveChildInvalidation = 0;
    volatile long mInvalidationTime = 0;
    volatile long mValidationTime = 0;
    volatile long mDirectValidationTime = 0;
    volatile long mRecursiveValidationTime = 0;
    volatile boolean mIsFile;

    SyncState(boolean isFile) {
      mIsFile = isFile;
    }

    void setInvalidationTime(long time) {
      if (time > mInvalidationTime) {
        mInvalidationTime = time;
      }
    }

    void setDirectChildInvalidation(long time) {
      if (time > mDirectChildInvalidation) {
        mDirectChildInvalidation = time;
      }
    }

    void setRecursiveChildInvalidation(long time) {
      if (time > mRecursiveChildInvalidation) {
        mRecursiveChildInvalidation = time;
      }
    }

    void setIsFile(boolean isFile) {
      // only transition from file to directory
      // and not the other way around
      if (!isFile && mIsFile) {
        mIsFile = false;
      }
    }

    void setValidationTime(long time, DescendantType descendantType) {
      if (time > mValidationTime) {
        mValidationTime = time;
      }
      if (descendantType == DescendantType.ALL) {
        if (time > mRecursiveValidationTime) {
          mRecursiveValidationTime = time;
        }
        if (time > mDirectValidationTime) {
          mDirectValidationTime = time;
        }
      } else if (descendantType == DescendantType.ONE) {
        if (time > mDirectValidationTime) {
          mDirectValidationTime = time;
        }
      }
    }
  }

  /**
   * Called when starting a sync.
   * @return the time at the start of the sync
   */
  public long recordStartSync() {
    return mClock.millis();
  }

  /**
   * Get sync times for a given path if they exist in the cache.
   * @param path the path to check
   * @return a pair of sync times, where element 0 is the direct sync time
   * and element 1 is the recursive sync time
   */
  public Optional<Pair<Long, Long>> getSyncTimesForPath(AlluxioURI path) {
    SyncState state = path.getPath().equals(AlluxioURI.SEPARATOR)
        ? mRoot : mItems.getIfPresent(path.getPath());
    return Optional.ofNullable(state).map(syncState ->
        new Pair<>(syncState.mValidationTime, syncState.mRecursiveValidationTime));
  }

  /**
   * Check if sync should happen.
   * A path is checked starting from the full path, all the way up to the root.
   * At each path the largest validation time and invalidation time is computed depending
   * on its descendant type. After each path component is checked, if the last invalidation
   * time is more recent than the last validation time then a sync is needed. Otherwise,
   * a sync is needed based on the difference between
   * the current time and the last sync time and the interval.
   * @param path the path to check
   * @param intervalMs the frequency in ms that the sync should happen
   * @param descendantType the descendant type of the operation being performed
   * @return a {@link SyncCheck} object indicating if a sync should be performed
   */
  public SyncCheck shouldSyncPath(AlluxioURI path, long intervalMs, DescendantType descendantType)
      throws InvalidPathException {
    int parentLevel = 0;
    String currPath = path.getPath();

    long lastValidationTime = 0;
    long lastInvalidationTime = 0;
    SyncState syncState;
    boolean isFile = false;
    while (true) {
      syncState = currPath.equals(AlluxioURI.SEPARATOR) ? mRoot : mItems.getIfPresent(currPath);
      if (syncState != null) {
        // we always check if the current path has been invalidated
        lastInvalidationTime = Math.max(lastInvalidationTime,
            syncState.mInvalidationTime);
        switch (parentLevel) {
          case 0: // the base path
            isFile = syncState.mIsFile;
            switch (descendantType) {
              case NONE:
                // we are syncing no children, so we use our validation time
                lastValidationTime = Math.max(lastValidationTime, syncState.mValidationTime);
                break;
              case ONE:
                // if the last sync on this path was a file, then we can use
                // the normal validation time, as it has no children
                lastValidationTime = Math.max(lastValidationTime, isFile
                    ? syncState.mValidationTime : syncState.mDirectValidationTime);
                // since we are syncing the children, we must check if a child was invalidated
                lastInvalidationTime = Math.max(lastInvalidationTime,
                    syncState.mDirectChildInvalidation);
                break;
              case ALL:
                // if the last sync on this path was a file, then we can use
                // the normal validation time, as it has no children
                lastValidationTime = Math.max(lastValidationTime, isFile
                    ? syncState.mValidationTime : syncState.mRecursiveValidationTime);
                // since we are syncing recursively, we must check if any recursive
                // child was invalidated
                lastInvalidationTime = Math.max(lastInvalidationTime,
                    syncState.mRecursiveChildInvalidation);
                break;
              default:
                throw new RuntimeException("Unexpected descendant type " + descendantType);
            }
            break;
          case 1: // at the parent path
            lastValidationTime = Math.max(lastValidationTime,
                isFile ? syncState.mDirectValidationTime :
                descendantType != DescendantType.NONE ? syncState.mRecursiveValidationTime
                    : syncState.mDirectValidationTime);
            break;
          default: // at a higher ancestor path
            lastValidationTime = Math.max(lastValidationTime, syncState.mRecursiveValidationTime);
        }
      }
      if (currPath.equals(AlluxioURI.SEPARATOR)) {
        break;
      }
      currPath = PathUtils.getParentCleaned(currPath);
      parentLevel++;
    }
    long currentTime = mClock.millis();
    SyncCheck result;

    if (intervalMs == 0) {
      // Always sync.
      return SyncCheck.shouldSyncWithTime(lastValidationTime);
    }
    if (lastInvalidationTime == 0 && intervalMs < 0) {
      // if no invalidation, and a negative interval then do not sync
      result = SyncCheck.shouldNotSyncWithTime(lastValidationTime);
    } else if (lastInvalidationTime >= lastValidationTime) {
      // if a recent invalidation then we always sync
      result = SyncCheck.shouldSyncWithTime(lastValidationTime);
    } else if (intervalMs < 0) {
      // do not sync with a negative interval
      result = SyncCheck.shouldNotSyncWithTime(lastValidationTime);
    } else if ((currentTime - lastValidationTime) >= intervalMs) {
      // syncing is needed based on an interval
      result = SyncCheck.shouldSyncWithTime(lastValidationTime);
    } else {
      // syncing is not needed based on an interval
      result = SyncCheck.shouldNotSyncWithTime(lastValidationTime);
    }
    LOG.debug("Result of should sync path {}: {}, invalidation time: {}, validation time: {},"
            + " clock time: {}",
        result, path, lastInvalidationTime, lastValidationTime, currentTime);
    return result;
  }

  /**
   * Notify that a UFS path has been invalidated.
   * @param ufsPath the ufs path
   */
  public void notifyUfsInvalidation(AlluxioURI ufsPath) throws InvalidPathException {
    Optional<AlluxioURI> path = mReverseResolution.apply(ufsPath);
    if (path.isPresent()) {
      notifyInvalidation(path.get());
    } else {
      LOG.warn("Received cross cluster invalidation for non mounted ufs path {}", ufsPath);
    }
  }

  /**
   * Notify that a path has been invalidated.
   * @param path the path
   */
  @VisibleForTesting
  public void notifyInvalidation(AlluxioURI path) throws InvalidPathException {
    String currPath = path.getPath();
    long time = mClock.millis();
    notifyInvalidationInternal(currPath, time);
  }

  private void notifyInvalidationInternal(String currPath, long time)
      throws InvalidPathException {
    LOG.debug("Set sync invalidation for path {} at time {}", currPath, time);
    int parentLevel = 0;

    if (currPath.equals(AlluxioURI.SEPARATOR)) {
      try (LockResource ignored = new LockResource(mRootLock)) {
        mRoot.setInvalidationTime(time);
      }
    } else {
      mItems.asMap().compute(currPath, (key, state) -> {
        if (state == null) {
          state = new SyncState(false);
        }
        // set the invalidation time for the path
        state.setInvalidationTime(time);
        return state;
      });
    }
    while (!currPath.equals(AlluxioURI.SEPARATOR)) {
      currPath = PathUtils.getParentCleaned(currPath);
      parentLevel++;
      if (currPath.equals(AlluxioURI.SEPARATOR)) {
        try (LockResource ignored = new LockResource(mRootLock)) {
          updateParentInvalidation(mRoot, time, parentLevel);
        }
      } else {
        final int finalParentLevel = parentLevel;
        mItems.asMap().compute(currPath, (key, state) -> {
          if (state == null) {
            state = new SyncState(false);
          }
          updateParentInvalidation(state, time, finalParentLevel);
          return state;
        });
      }
    }
  }

  private void updateParentInvalidation(SyncState state, long time, long parentLevel) {
    state.setIsFile(false);
    if (parentLevel == 1) {
      // the parent has had a direct child invalidated
      state.setDirectChildInvalidation(time);
    }
    // the parent has had a recursive child invalidated
    state.setRecursiveChildInvalidation(time);
  }

  /**
   * Notify sync happened.
   * @param path the synced path
   * @param descendantType the descendant type for the performed operation
   * @param startTime the time at which the sync was started
   * @param syncTime the time to set the sync success to, if null then the current
   *                 clock time is used
   * @param isFile true if the synced path is a file
   */
  public void notifySyncedPath(
      AlluxioURI path, DescendantType descendantType, long startTime, @Nullable Long syncTime,
      boolean isFile) {
    long time = syncTime == null ? startTime :
        Math.min(startTime, syncTime);
    if (path.getPath().equals(AlluxioURI.SEPARATOR)) {
      try (LockResource ignored = new LockResource(mRootLock)) {
        Preconditions.checkState(!isFile);
        updateSyncState(mRoot, time, startTime, false, descendantType);
      }
    } else {
      mItems.asMap().compute(path.getPath(), (key, state) -> {
        if (state == null) {
          state = new SyncState(isFile);
        }
        updateSyncState(state, time, startTime, isFile, descendantType);
        return state;
      });
    }
  }

  private void updateSyncState(
      SyncState state, long time, long startTime, boolean isFile, DescendantType descendantType) {
    state.setIsFile(isFile);
    state.setValidationTime(time, descendantType);
    // we can reset the invalidation times, as long as we have not received
    // an invalidation since the sync was started
    if (descendantType == DescendantType.ALL && state.mInvalidationTime < startTime) {
      state.mInvalidationTime = 0;
    }
    if (descendantType == DescendantType.ALL && state.mRecursiveChildInvalidation < startTime) {
      state.mRecursiveChildInvalidation = 0;
    }
    if ((descendantType == DescendantType.ALL || descendantType == DescendantType.ONE)
        && state.mDirectChildInvalidation < startTime) {
      state.mDirectChildInvalidation = 0;
    }
  }
}
