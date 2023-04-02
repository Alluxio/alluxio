package alluxio.master.metastore.rocks;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * This is a handle used to close the write lock(exclusive lock) on RocksStore.
 * The exclusive lock is acquired when ref count is zero, and the StopServingFlag ensures
 * no new r/w will come in, so the ref count will stay zero throughout the period.
 */
public class RocksWriteLockHandle implements AutoCloseable {
  /*
   * true: the RocksDB is temporarily closed and can serve after the lock is released
   * false: the RocksDB is closed permanently because the master is shutting down
   */
  final boolean mIsRestart;
  final AtomicBoolean mStopServingFlag;
  final LongAdder mRefCount;

  /**
   * The constructor.
   *
   * @param isRestart if true, reset the StopServingFlag so the RocksDB accepts new operations
   * @param stopServingFlag the flag on RocksStore
   * @param refCount the ref count on RocksStore
   */
  public RocksWriteLockHandle(
      boolean isRestart, AtomicBoolean stopServingFlag, LongAdder refCount) {
    mIsRestart = isRestart;
    mStopServingFlag = stopServingFlag;
    mRefCount = refCount;
  }

  @Override
  public void close() {
    Preconditions.checkState(mRefCount.sum() == 0,
        "Some read/write operations did not respect the write lock on the RocksStore "
            + "and messed up the ref count! Current ref count is %s", mRefCount.sum());
    if (mIsRestart) {
      // Set the StopServingFlag back to false so r/w operations can be accepted
      mStopServingFlag.set(false);
    }
  }
}
