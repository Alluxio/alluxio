package alluxio.master.metastore.rocks;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * This is a handle used to close a read lock(shared lock) on RocksStore.
 * RocksStore uses ref count for locking so releasing a read lock is just decrementing the
 * reference count.
 */
public class RocksSharedLockHandle implements AutoCloseable {
  final LongAdder mRefCount;
  final AtomicReference<VersionedRocksStoreStatus> mStatus;
  final VersionedRocksStoreStatus mLockedVersion;

  /**
   * The constructor.
   *
   * @param refCount the ref count to decrement on close
   */
  public RocksSharedLockHandle(AtomicReference<VersionedRocksStoreStatus> status, LongAdder refCount) {
    mStatus = status;
    mRefCount = refCount;
    mLockedVersion = mStatus.get();
  }

  @Override
  public void close() {
    if (mStatus.get() == mLockedVersion) {
      mRefCount.decrement();
    }
    /*
     * If the version(ref) has changed, that means the exclusive lock has been forced by the closer
     * and the ref count has been reset to zero. In that case, the lock should not decrement
     * the ref count because the count has been reset.
     */
  }
}
