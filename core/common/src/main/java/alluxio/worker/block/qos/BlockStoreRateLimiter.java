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

package alluxio.worker.block.qos;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.qos.RateLimiter;
import alluxio.resource.LockResource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link RateLimiter} For BlockStore.
 */
public class BlockStoreRateLimiter implements RateLimiter {
  private static final Lock READ_LIMITER_INIT_LOCK = new ReentrantLock();
  private static final Lock WRITE_LIMITER_INIT_LOCK = new ReentrantLock();
  private static BlockStoreRateLimiter sReadLimiter;
  private static BlockStoreRateLimiter sWriteLimiter;

  private final com.google.common.util.concurrent.RateLimiter mRateLimiter;

  /**
   * Constructs or get read limiter.
   * @param conf conf
   * @return read limiter
   */
  public static BlockStoreRateLimiter getReadLimiter(AlluxioConfiguration conf) {
    if (sReadLimiter == null) {
      try (LockResource ignored = new LockResource(READ_LIMITER_INIT_LOCK)) {
        if (sReadLimiter == null) {
          sReadLimiter = new BlockStoreRateLimiter(getThroughputLimit(conf, TpsLimitType.READ));
        }
      }
    }
    return sReadLimiter;
  }

  /**
   * Constructs or get write limiter.
   * @param conf conf
   * @return write limiter
   */
  public static BlockStoreRateLimiter getWriteLimiter(AlluxioConfiguration conf) {
    if (sWriteLimiter == null) {
      try (LockResource ignored = new LockResource(WRITE_LIMITER_INIT_LOCK)) {
        if (sWriteLimiter == null) {
          sWriteLimiter = new BlockStoreRateLimiter(getThroughputLimit(conf, TpsLimitType.WRITE));
        }
      }
    }
    return sWriteLimiter;
  }

  private BlockStoreRateLimiter(double limits) {
    mRateLimiter = com.google.common.util.concurrent.RateLimiter.create(limits);
  }

  /**
   * Clear the read limiter & write limiter.
   */
  public static void clear() {
    if (sReadLimiter != null) {
      try (LockResource ignored = new LockResource(READ_LIMITER_INIT_LOCK)) {
        if (sReadLimiter != null) {
          sReadLimiter = null;
        }
      }
    }
    if (sWriteLimiter != null) {
      try (LockResource ignored = new LockResource(WRITE_LIMITER_INIT_LOCK)) {
        if (sWriteLimiter != null) {
          sWriteLimiter = null;
        }
      }
    }
  }

  @Override
  public void acquire(int permits) {
    mRateLimiter.acquire(permits);
  }

  @Override
  public boolean tryAcquire(int permits) {
    return mRateLimiter.tryAcquire(permits);
  }

  @Override
  public boolean tryAcquire(int permits, long timeOut, TimeUnit timeUnit) {
    return mRateLimiter.tryAcquire(permits, timeOut, timeUnit);
  }

  private static double getThroughputLimit(AlluxioConfiguration conf, TpsLimitType type) {
    long throughput;
    switch (type) {
      case READ:
        throughput = conf.getBytes(PropertyKey.WORKER_LOCAL_BLOCK_READ_THROUGHPUT);
        break;
      case WRITE:
        throughput = conf.getBytes(PropertyKey.WORKER_LOCAL_BLOCK_WRITE_THROUGHPUT);
        break;
      default:
        throw new IllegalArgumentException("UNKNOWN TpsLimitType!");
    }
    return throughput;
  }

  private enum TpsLimitType {
    READ,
    WRITE
  }
}
