package alluxio.master.metastore.rocks;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * This is a handle used to manage the write lock(exclusive lock) on RocksStore.
 * The exclusive lock is acquired when ref count is zero, and the StopServingFlag ensures
 * no new r/w will come in, so the ref count will stay zero throughout the period.
 */
public class RocksExclusiveLockHandle implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RocksExclusiveLockHandle.class);
  private static final boolean TEST_MODE = Configuration.getBoolean(PropertyKey.TEST_MODE);

  final UnlockAction mUnlockAction;
  final AtomicStampedReference<Boolean> mStatus;
  final LongAdder mRefCount;
  final AtomicInteger mVersionedRefCountTracker;

  /**
   * The constructor.
   *
   * @param refCount the ref count on RocksStore
   */
  public RocksExclusiveLockHandle(UnlockAction unlockAction,
                                  AtomicStampedReference<Boolean> status,
                                  LongAdder refCount,
                                  AtomicInteger refCountVersionTracker) {
    mUnlockAction = unlockAction;
    mStatus = status;
    mRefCount = refCount;
    mVersionedRefCountTracker = refCountVersionTracker;
  }

  @Override
  public void close() {
    if (TEST_MODE) {
      // In test mode we enforce strict ref count check, as a canary for ref count issues
      Preconditions.checkState(mRefCount.sum() == 0,
          "Some read/write operations did not respect the write lock on the RocksStore "
              + "and messed up the ref count! Current ref count is %s", mRefCount.sum());
    } else {
      // In a real deployment, we forgive potential ref count problems and take the risk
      long refCount = mRefCount.sum();
      if (refCount != 0) {
        LOG.warn("Some read/write operations did not respect the write lock on the RocksStore "
            + "and messed up the ref count! Current ref count is {}", refCount);
      }
      mRefCount.reset();
      // Mark the version so observers will know the ref count has been reset
      mVersionedRefCountTracker.incrementAndGet();
    }

    switch (mUnlockAction) {
      case RESET_NEW_VERSION:
        mStatus.set(false, mStatus.getStamp() + 1);
        break;
      case RESET_SAME_VERSION:
        mStatus.set(false, mStatus.getStamp());
        break;
      case NO_OP:
        break;
      default:
        throw new IllegalArgumentException("Unrecognized enum " + mUnlockAction);
    }
  }

  /**
   * Defines a bunch of actions to take when the exclusive lock is released.
   */
  public enum UnlockAction {
    // No need to reset the flag when the lock is released. The process is exiting anyway.
    NO_OP,
    // When the lock is released, the RocksDB contains the same contents.
    // This lock was used for writing a checkpoint.
    RESET_SAME_VERSION,
    // When the lock is released, the RocksDB contains different contents so old readers
    // must abort.
    RESET_NEW_VERSION;
  }
}
