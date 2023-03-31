package alluxio.master.metastore.rocks;

import javax.annotation.Nullable;

public class RocksWriteLock implements AutoCloseable {
  final Runnable mCloseAction;

  public RocksWriteLock(@Nullable Runnable runnable) {
    mCloseAction = runnable;
  }

  @Override
  public void close() {
    if (mCloseAction != null) {
      mCloseAction.run();
    }
  }
}
