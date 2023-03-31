/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.rocks;

import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.master.metastore.rocks.RocksStore;
import alluxio.resource.LockResource;
import alluxio.retry.CountingSleepRetry;
import alluxio.util.SleepUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// TODO(jiacheng): This benchmark tool is only used to compare different locking mechanisms
//  This class shall be removed in the end because we have picked one best one
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
  public static class LongObject {
    int mValue = ThreadLocalRandom.current().nextInt();
    final AtomicBoolean mFlag = new AtomicBoolean(true);
    final LongAdder mVal = new LongAdder();

    class LongResource implements AutoCloseable {

      LongResource() {
        if (!mFlag.get()) {
          throw new UnavailableRuntimeException("failure");
        }
        mVal.increment();
        if (!mFlag.get()) {
          mVal.decrement();
          // you must also throw the exception here because someone could have
          // closed the item after it was closed
          throw new UnavailableRuntimeException("failure");
        }
      }

      @Override
      public void close() {
        mVal.decrement();
      }
    }

    public int getObjectField() {
      try (LongResource lock = new LongResource()) {
        return mValue;
      }
    }

    @TearDown(Level.Trial)
    public void closeObject() {
      // TODO(jiacheng): consider if the version is necessary
      mFlag.compareAndSet(true, false);

      // TODO(jiacheng): grace period


      // TODO(jiacheng): configurable
      Instant waitStart = Instant.now();
      // Wait until:
      // 1. Ref count is zero, meaning all concurrent r/w have completed or aborted
      // 2. Timeout is reached, meaning we force close/restart without waiting
      CountingSleepRetry retry = new CountingSleepRetry(5, 1000);
      while (mVal.longValue() != 0 && retry.attempt()) {
        SleepUtils.sleepMs(1000);
      }
      Duration elapsed = Duration.between(waitStart, Instant.now());
    }
  }


  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(40)
  @Benchmark
  public void getSynchronized(SynchronizedObject so, Blackhole bh) {
    for (int i = 0; i < 10_000; i++) {
      bh.consume(so.getObjectField());
    }
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(40)
  @Benchmark
  public void getReadLock(LockedObject lo, Blackhole bh) {
    for (int i = 0; i < 10_000; i++) {
      bh.consume(lo.getObjectField());
    }
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(40)
  @Benchmark
  public int getReferenceLock(LongObject lo, Blackhole bh) throws Exception {
    int counter = 0;
    for (int i = 0; i < 10_000; i++) {
      bh.consume(lo.getObjectField());
    }
    return counter;
  }
}
