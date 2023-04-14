package alluxio.master.metastore.rocks;

import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * This is a handle used to close a read lock(shared lock) on RocksStore.
 * RocksStore uses ref count for locking so releasing a read lock is just decrementing the
 * reference count.
 */
public class RocksSharedLockHandle implements AutoCloseable {
  final LongAdder mRefCount;
  final AtomicStampedReference<Boolean> mStopServing;
  final int mLockedVersion;

  /**
   * The constructor.
   *
   * @param refCount the ref count to decrement on close
   */
  public RocksSharedLockHandle(AtomicStampedReference<Boolean> stopServingFlag, LongAdder refCount) {
    mStopServing = stopServingFlag;
    mRefCount = refCount;
    mLockedVersion = mStopServing.getStamp();
  }

  @Override
  public void close() {
    if (mStopServing.getStamp() == mLockedVersion) {
      mRefCount.decrement();
    }
    /*
     * If the version has changed, that means the exclusive lock has been forced by the closer
     * and the ref count has been reset to zero. In that case, the lock should not decrement
     * the ref count because the count has been reset.
     */
  }
}
