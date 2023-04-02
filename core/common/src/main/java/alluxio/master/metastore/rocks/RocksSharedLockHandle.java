package alluxio.master.metastore.rocks;

import java.util.concurrent.atomic.LongAdder;

/**
 * This is a handle used to close a read lock(shared lock) on RocksStore.
 * RocksStore uses ref count for locking so releasing a read lock is just decrementing the
 * reference count.
 */
public class RocksSharedLockHandle implements AutoCloseable {
  final LongAdder mRefCount;

  /**
   * The constructor.
   *
   * @param refCount the ref count to decrement on close
   */
  public RocksSharedLockHandle(LongAdder refCount) {
    mRefCount = refCount;
  }

  @Override
  public void close() {
    mRefCount.decrement();
  }
}
