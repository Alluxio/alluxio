package alluxio.master.file.meta.crosscluster;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;
import alluxio.master.file.meta.SyncPathCache;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A cache of path invalidations.
 */
public class InvalidationSyncCache implements SyncPathCache {
  private static final Logger LOG = LoggerFactory.getLogger(InvalidationSyncCache.class);

  private final ConcurrentHashMap<String, SyncState> mItems = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> mActiveSyncs = new ConcurrentHashMap<>();

  private final AtomicLong mTime = new AtomicLong();

  private static class SyncState {
    long mDirectChildInvalidation;
    long mRecursiveChildInvalidation;
    long mInvalidationTime;
    long mValidationTime;
    long mDirectValidation;
    long mRecursiveValidationTime;
    long mIsFile;

    void setInvalidationTime(long time) {
      mInvalidationTime = time;
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

    void setValidationTime(long time, DescendantType descendantType) {
      mValidationTime = time;
      if (descendantType == DescendantType.ALL) {
        mRecursiveValidationTime = time;
        mDirectValidation = time;
      } else if (descendantType == DescendantType.ONE) {
        mDirectValidation = time;
      }
    }

    SyncState createCopy() {
      SyncState copy = new SyncState();
      copy.mDirectChildInvalidation = mDirectChildInvalidation;
      copy.mRecursiveChildInvalidation = mRecursiveChildInvalidation;
      copy.mInvalidationTime = mInvalidationTime;
      copy.mValidationTime = mValidationTime;
      copy.mDirectValidation = mDirectValidation;
      copy.mRecursiveValidationTime = mRecursiveValidationTime;
      copy.mIsFile = mIsFile;
      return copy;
    }
  }

  /**
   * Called before a sync is started on a path.
   * @param path
   */
  @Override
  public void startSync(AlluxioURI path) {
    mActiveSyncs.put(path.getPath(), mTime.incrementAndGet());
  }

  @Override
  public void failedSyncPath(AlluxioURI path) {
    Objects.requireNonNull(mActiveSyncs.remove(path.getPath()));
  }

  /**
   * Checks if the path should be synced.
   * @param path
   * @param intervalMs
   * @param descendantType
   * @return if the path should be synced
   */
  @Override
  public boolean shouldSyncPath(AlluxioURI path, long intervalMs, DescendantType descendantType) {
    int parentLevel = 0;
    String currPath = path.getPath();

    long lastValidationTime = 0;
    long lastInvalidationTime = 0;
    SyncState syncState;
    while (true) {
      syncState = mItems.get(currPath);
      if (syncState != null) {
        switch (parentLevel) {
          case 0:
            lastInvalidationTime = Math.max(lastInvalidationTime,
                syncState.mInvalidationTime);
            switch (descendantType) {
              case NONE:
                lastValidationTime = Math.max(lastValidationTime, syncState.mValidationTime);
                break;
              case ONE:
                lastValidationTime = Math.max(lastValidationTime, syncState.mDirectValidation);
                lastInvalidationTime = Math.max(lastInvalidationTime,
                    syncState.mDirectChildInvalidation);
                break;
              case ALL:
                lastValidationTime = Math.max(lastValidationTime,
                    syncState.mRecursiveValidationTime);
                lastInvalidationTime = Math.max(lastInvalidationTime,
                    syncState.mRecursiveChildInvalidation);
                break;
              default:
                throw new RuntimeException("Unexpected descendant type " + descendantType);
            }
            break;
          case 1:
            lastValidationTime = Math.max(lastValidationTime,
                descendantType != DescendantType.NONE ? syncState.mRecursiveValidationTime
                    : syncState.mDirectValidation);
            break;
          default:
            lastValidationTime = Math.max(lastValidationTime, syncState.mRecursiveValidationTime);
        }
      }
      if (currPath.equals(AlluxioURI.SEPARATOR)) {
        break;
      }
      try {
        currPath = PathUtils.getParent(currPath);
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
      parentLevel++;
    }
    return lastInvalidationTime >= lastValidationTime;
  }

  /**
   * Notify that a path has been invalidated.
   * @param path
   * @throws InvalidPathException
   */
  public void notifyInvalidation(AlluxioURI path) throws InvalidPathException {
    int parentLevel = 0;
    String currPath = path.getPath();
    long time = mTime.incrementAndGet();
    mItems.compute(currPath, (key, state) -> {
      if (state == null) {
        state = new SyncState();
      }
      state.setInvalidationTime(time);
      return state;
    });

    while (!currPath.equals(AlluxioURI.SEPARATOR)) {
      currPath = PathUtils.getParent(currPath);
      parentLevel++;
      final int finalParentLevel = parentLevel;
      mItems.compute(currPath, (key, state) -> {
        if (state == null) {
          state = new SyncState();
        } else {
          // use copy on write for concurrency
          state = state.createCopy();
        }
        if (finalParentLevel == 1) {
          state.setDirectChildInvalidation(time);
        }
        state.setRecursiveChildInvalidation(time);
        return state;
      });
    }
  }

  /**
   * Called when a path is synced.
   * @param path
   * @param descendantType
   */
  @Override
  public void notifySyncedPath(AlluxioURI path, DescendantType descendantType) {
    // assume if descendantType is ONE or NONE then one level of descendants
    // are always synced anyway
    final long syncTime = Objects.requireNonNull(mActiveSyncs.remove(path.getPath()));
    mItems.compute(path.getPath(), (key, state) -> {
      if (state == null) {
        state = new SyncState();
      } else {
        state = state.createCopy();
      }
      state.setValidationTime(syncTime, descendantType);
      if (descendantType == DescendantType.ALL && state.mRecursiveChildInvalidation < syncTime) {
        state.mRecursiveChildInvalidation = 0;
      }
      if ((descendantType == DescendantType.ALL || descendantType == DescendantType.ONE)
          && state.mDirectChildInvalidation < syncTime) {
        state.mDirectChildInvalidation = 0;
      }
      return state;
    });
  }
}
