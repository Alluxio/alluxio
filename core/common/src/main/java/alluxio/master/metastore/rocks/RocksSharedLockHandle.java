package alluxio.master.metastore.rocks;

import java.util.concurrent.atomic.LongAdder;

/**
 * This is a handle used to manage a read lock(shared lock) on RocksStore.
 * RocksStore uses ref count for locking so releasing a read lock is just decrementing the
 * reference count.
 */
public class RocksSharedLockHandle implements AutoCloseable {
  final int mDbVersion;
  final LongAdder mRefCount;

  /**
   * The constructor.
   *
   * @param refCount the ref count to decrement on close
   */
  public RocksSharedLockHandle(int dbVersion, LongAdder refCount) {
    mDbVersion = dbVersion;
    mRefCount = refCount;
  }

  @Override
  public void close() {
    /*
     * If the exclusive lock has been forced and the ref count is reset, this reference will point
     * to an out-of-date counter. Therefore, we can just update this counter without concerns.
     * If the exclusive lock is has NOT been forced, we decrement the ref count normally.
     * If the exclusive lock has been forced, we decrement an irrelevant counter which will never
     * be read.
     */
    mRefCount.decrement();
  }
}
