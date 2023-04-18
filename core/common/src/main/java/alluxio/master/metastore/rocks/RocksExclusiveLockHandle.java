package alluxio.master.metastore.rocks;

import alluxio.exception.runtime.AlluxioRuntimeException;

import java.util.concurrent.Callable;

/**
 * This is a handle used to manage the write lock(exclusive lock) on RocksStore.
 * The exclusive lock is acquired when ref count is zero, and the StopServingFlag ensures
 * no new r/w will come in, so the ref count will stay zero throughout the period.
 */
public class RocksExclusiveLockHandle implements AutoCloseable {
  final Callable<Void> mCloseAction;

  /**
   * The constructor.
   *
   */
  public RocksExclusiveLockHandle(Callable<Void> closeAction) {
    mCloseAction = closeAction;
  }

  @Override
  public void close() {
    try {
      mCloseAction.call();
    } catch (Exception e) {
      // From the current usage in RocksStore, this is unreachable
      throw AlluxioRuntimeException.from(e);
    }
  }
}
