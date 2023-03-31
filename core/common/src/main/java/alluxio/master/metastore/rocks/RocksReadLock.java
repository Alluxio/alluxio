package alluxio.master.metastore.rocks;

import java.util.concurrent.atomic.LongAdder;

// TODO(jiacheng): instead of a lock, this is more like a closing action handle
//  so rename it
public class RocksReadLock implements AutoCloseable {
  final LongAdder mRefCount;
  public RocksReadLock(LongAdder refCount) {
    mRefCount = refCount;
  }

  @Override
  public void close() {
    mRefCount.decrement();
  }
}
