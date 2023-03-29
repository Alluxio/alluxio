package alluxio.rocks;

import alluxio.concurrent.CountingLatch;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.master.metastore.rocks.RocksReadLock;
import alluxio.master.metastore.rocks.RocksRefCountReadLock;
import alluxio.resource.LockResource;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RocksStoreBench {
  @State(Scope.Benchmark)
  public static class SynchronizedObject {
    int mValue = ThreadLocalRandom.current().nextInt();

    public synchronized int getObjectField() {
      return mValue;
    }
  }

  @State(Scope.Benchmark)
  public static class LockedObject {
    int mValue = ThreadLocalRandom.current().nextInt();
    final ReentrantReadWriteLock mLock = new ReentrantReadWriteLock();
    final AtomicBoolean mFlag = new AtomicBoolean(true);

    public int getObjectField() {
      if (!mFlag.get()) {
        throw new UnavailableRuntimeException("failure");
      }
      try (LockResource lock = new LockResource(mLock.readLock())) {
        if (!mFlag.get()) {
          throw new UnavailableRuntimeException("failure");
        }
        return mValue;
      }
    }
  }

  @State(Scope.Benchmark)
  public static class CountingLatchObject {
    int mValue = ThreadLocalRandom.current().nextInt();
    final CountingLatch mLock = new CountingLatch();
    final AtomicBoolean mFlag = new AtomicBoolean(true);

    public int getObjectField() {
      if (!mFlag.get()) {
        throw new UnavailableRuntimeException("failure");
      }
      try {
        mLock.inc();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      try (RocksReadLock lock = new RocksReadLock(mLock)) {
        if (!mFlag.get()) {
          throw new UnavailableRuntimeException("failure");
        }
        return mValue;
      }
    }
  }

  @State(Scope.Benchmark)
  public static class RefCountObject {
    int mValue = ThreadLocalRandom.current().nextInt();
    final AtomicInteger mRefCount = new AtomicInteger(0);
    final AtomicBoolean mFlag = new AtomicBoolean(false);
    final RocksRefCountReadLock mLock = new RocksRefCountReadLock(mFlag, mRefCount);

    public int getObjectField() {
      try (RocksRefCountReadLock.RocksRefCountReadLockHandle lock = mLock.lock()) {
        return mValue;
      }
    }
  }


  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(40)
  @Benchmark
  public int getSynchronized(SynchronizedObject so) {
    int counter = 0;
    for (int i = 0; i < 10_000; i++) {
      counter += so.getObjectField();
    }
    return counter;
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(40)
  @Benchmark
  public int getReadLock(LockedObject lo) {
    int counter = 0;
    for (int i = 0; i < 10_000; i++) {
      counter += lo.getObjectField();
    }
    return counter;
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(40)
  @Benchmark
  public int getLatchLock(CountingLatchObject lo) {
    int counter = 0;
    for (int i = 0; i < 10_000; i++) {
      counter += lo.getObjectField();
    }
    return counter;
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(40)
  @Benchmark
  public int getRefCountLock(RefCountObject lo) {
    int counter = 0;
    for (int i = 0; i < 10_000; i++) {
      counter += lo.getObjectField();
    }
    return counter;
  }
}
