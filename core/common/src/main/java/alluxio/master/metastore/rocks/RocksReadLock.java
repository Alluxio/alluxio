package alluxio.master.metastore.rocks;

import java.util.concurrent.atomic.AtomicInteger;

// TODO(jiacheng): instead of a lock, this is more like a closing action handle
public class RocksReadLock implements AutoCloseable {
  final AtomicInteger mRefCount;
  public RocksReadLock(AtomicInteger refCount) {
    mRefCount = refCount;
  }

  @Override
  public void close() {
    mRefCount.decrementAndGet();
  }
}
