package alluxio.master.metastore.rocks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * This is a handle used to manage a read lock(shared lock) on RocksStore.
 * RocksStore uses ref count for locking so releasing a read lock is just decrementing the
 * reference count.
 */
public class RocksSharedLockHandle implements AutoCloseable {
  final LongAdder mRefCount;
  final AtomicInteger mVersionedRefCountTracker;
  final int mDbVersion;
  final int mLockedRefCountVersion;

  /**
   * The constructor.
   *
   * @param refCount the ref count to decrement on close
   */
  public RocksSharedLockHandle(int dbVersion, LongAdder refCount,
                               AtomicInteger refCountVersionTracker) {
    mDbVersion = dbVersion;
    mRefCount = refCount;
    mVersionedRefCountTracker = refCountVersionTracker;
    mLockedRefCountVersion = refCountVersionTracker.get();
  }

  @Override
  public void close() {
    /*
     * If the version(ref) has changed, that means the exclusive lock has been forced by the closer
     * and the ref count has been reset to zero. In that case, the lock should not decrement
     * the ref count because the count has been reset.
     */
    if (mVersionedRefCountTracker.get() != mLockedRefCountVersion) {
      System.out.println("Ref counter version has changed. Do not update ref count!");
    } else {
      mRefCount.decrement();
    }
  }
}
