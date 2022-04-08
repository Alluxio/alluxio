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

package alluxio.worker.block;

import alluxio.Constants;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.ResourcePool;
import alluxio.util.logging.SamplingLogger;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class for managing block read/write locks. After obtaining a lock with
 * {@link ResourcePool#acquire()}, {@link ResourcePool#release(Object)} must be called when the
 * thread is done using the client.
 */
@ThreadSafe
public class BlockRWLockPool extends ResourcePool<ClientRWLock> {
  private static final Logger SAMPLING_LOG =
      new SamplingLogger(LoggerFactory.getLogger(BlockRWLockPool.class),
          30L * Constants.SECOND_MS);

  @VisibleForTesting
  final AtomicInteger mRemainingPoolResources;

  /**
   * Creates a new client read/write lock pool.
   * @param maxCapacity the max capacity of the pool
   */
  public BlockRWLockPool(int maxCapacity) {
    super(maxCapacity);
    mRemainingPoolResources = new AtomicInteger(maxCapacity);

    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.WORKER_BLOCK_LOCK_POOL_REMAINING_RESOURCES_COUNT.toString(),
        mRemainingPoolResources::get,
        2,
        TimeUnit.SECONDS
    );
  }

  /**
   * Do a timed acquire with 1 second timeout first.
   *
   */
  @Override
  public ClientRWLock acquire() {
    ClientRWLock ret = this.acquire(1, TimeUnit.SECONDS);
    if (ret != null) {
      return ret;
    }
    ret = this.acquire(WAIT_INDEFINITELY, null);
    return ret;
  }

  @Nullable
  @Override
  public ClientRWLock acquire(long time, TimeUnit unit) {
    ClientRWLock ret = super.acquire(time, unit);
    if (ret != null) {
      mRemainingPoolResources.decrementAndGet();
    } else {
      SAMPLING_LOG.error(
          "Lock acquisition failed due to lock pool resources being exhausted. "
              + "Pool capacity size {}; lock acquisition duration {}{}",
          mMaxCapacity,
          time,
          unit
      );
    }
    return ret;
  }

  @Override
  public void release(ClientRWLock resource) {
    mRemainingPoolResources.incrementAndGet();
    super.release(resource);
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public ClientRWLock createNewResource() {
    return new ClientRWLock();
  }
}
