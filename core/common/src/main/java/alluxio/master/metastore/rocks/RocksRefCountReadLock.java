package alluxio.master.metastore.rocks;

import alluxio.exception.runtime.UnavailableRuntimeException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksRefCountReadLock {
  final AtomicInteger mCounter;
  final AtomicBoolean mFlag;
  public RocksRefCountReadLock(AtomicBoolean flag, AtomicInteger counter) {
    mFlag = flag;
    mCounter = counter;
  }

  public RocksRefCountReadLockHandle lock() {
    // If flag is false then no one is competing for the lock
    // just increment the ref count and return an AutoCloseable for decrement
    if (mFlag.get()) {
      throw new UnavailableRuntimeException("shutting down");
    }

    mCounter.incrementAndGet();
    return new RocksRefCountReadLockHandle(mCounter);
  }

  public static class RocksRefCountReadLockHandle implements AutoCloseable {
    final AtomicInteger mCounter;

    RocksRefCountReadLockHandle(AtomicInteger counter) {
      mCounter = counter;
    }

    @Override
    public void close() {
      mCounter.decrementAndGet();
    }
  }
}
