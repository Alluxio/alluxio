/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.worker;

import alluxio.metrics.source.Source;
import alluxio.worker.block.BlockWorker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Worker source collects a worker's internal state.
 */
@NotThreadSafe
public class WorkerSource implements Source {
  private static final String WORKER_SOURCE_NAME = "worker";

  public static final String BLOCKS_ACCESSED = "BlocksAccessed";
  public static final String BLOCKS_CANCELED = "BlocksCanceled";
  public static final String BLOCKS_DELETED = "BlocksDeleted";
  public static final String BLOCKS_EVICTED = "BlocksEvicted";
  public static final String BLOCKS_PROMOTED = "BlocksPromoted";
  public static final String BLOCKS_READ_LOCAL = "BlocksReadLocal";
  public static final String BLOCKS_READ_REMOTE = "BlocksReadRemote";
  public static final String BLOCKS_WRITTEN_LOCAL = "BlocksWrittenLocal";
  public static final String BLOCKS_WRITTEN_REMOTE = "BlocksWrittenRemote";
  public static final String BYTES_READ_LOCAL = "BytesReadLocal";
  public static final String BYTES_READ_REMOTE = "BytesReadRemote";
  public static final String BYTES_READ_UFS = "BytesReadUfs";
  public static final String BYTES_WRITTEN_LOCAL = "BytesWrittenLocal";
  public static final String BYTES_WRITTEN_REMOTE = "BytesWrittenRemote";
  public static final String BYTES_WRITTEN_UFS = "BytesWrittenUfs";
  public static final String CAPACITY_TOTAL = "CapacityTotal";
  public static final String CAPACITY_USED = "CapacityUsed";
  public static final String CAPACITY_FREE = "CapacityFree";
  public static final String BLOCKS_CACHED = "BlocksCached";

  private boolean mGaugesRegistered = false;
  private final MetricRegistry mMetricRegistry = new MetricRegistry();
  private final Counter mBlocksAccessed =
      mMetricRegistry.counter(MetricRegistry.name(BLOCKS_ACCESSED));
  private final Counter mBlocksCanceled =
      mMetricRegistry.counter(MetricRegistry.name(BLOCKS_CANCELED));
  private final Counter mBlocksDeleted =
      mMetricRegistry.counter(MetricRegistry.name(BLOCKS_DELETED));
  private final Counter mBlocksEvicted =
      mMetricRegistry.counter(MetricRegistry.name(BLOCKS_EVICTED));
  private final Counter mBlocksPromoted =
      mMetricRegistry.counter(MetricRegistry.name(BLOCKS_PROMOTED));

  // metrics from client
  private final Counter mBlocksReadLocal = mMetricRegistry.counter(MetricRegistry
      .name(BLOCKS_READ_LOCAL));
  private final Counter mBlocksReadRemote = mMetricRegistry.counter(MetricRegistry
      .name(BLOCKS_READ_REMOTE));
  private final Counter mBlocksWrittenLocal = mMetricRegistry.counter(MetricRegistry
      .name(BLOCKS_WRITTEN_LOCAL));
  private final Counter mBlocksWrittenRemote = mMetricRegistry.counter(MetricRegistry
      .name(BLOCKS_WRITTEN_REMOTE));
  private final Counter mBytesReadLocal = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_READ_LOCAL));
  private final Counter mBytesReadRemote = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_READ_REMOTE));
  private final Counter mBytesReadUfs = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_READ_UFS));
  private final Counter mBytesWrittenLocal = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_WRITTEN_LOCAL));
  private final Counter mBytesWrittenRemote = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_WRITTEN_REMOTE));
  private final Counter mBytesWrittenUfs = mMetricRegistry.counter(MetricRegistry
      .name(BYTES_WRITTEN_UFS));

  @Override
  public String getName() {
    return WORKER_SOURCE_NAME;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return mMetricRegistry;
  }

  /**
   * Increments the counter of accessed blocks.
   *
   * @param n the increment
   */
  public void incBlocksAccessed(long n) {
    mBlocksAccessed.inc(n);
  }

  /**
   * Increments the counter of canceled blocks.
   *
   * @param n the increment
   */
  public void incBlocksCanceled(long n) {
    mBlocksCanceled.inc(n);
  }

  /**
   * Increments the counter of deleted blocks.
   *
   * @param n the increment
   */
  public void incBlocksDeleted(long n) {
    mBlocksDeleted.inc(n);
  }

  /**
   * Increments the counter of evicted blocks.
   *
   * @param n the increment
   */
  public void incBlocksEvicted(long n) {
    mBlocksEvicted.inc(n);
  }

  /**
   * Increments the counter of promoted blocks.
   *
   * @param n the increment
   */
  public void incBlocksPromoted(long n) {
    mBlocksPromoted.inc(n);
  }

  /**
   * Increments the counter of blocks read locally.
   *
   * @param n the increment
   */
  public void incBlocksReadLocal(long n) {
    mBlocksReadLocal.inc(n);
  }

  /**
   * Increments the counter of blocks read remotely.
   *
   * @param n the increment
   */
  public void incBlocksReadRemote(long n) {
    mBlocksReadRemote.inc(n);
  }

  /**
   * Increments the counter of blocks written locally.
   *
   * @param n the increment
   */
  public void incBlocksWrittenLocal(long n) {
    mBlocksWrittenLocal.inc(n);
  }

  /**
   * Increments the counter of blocks written remotely.
   *
   * @param n the increment
   */
  public void incBlocksWrittenRemote(long n) {
    mBlocksWrittenRemote.inc(n);
  }

  /**
   * Increments the counter of bytes read locally.
   *
   * @param n the increment
   */
  public void incBytesReadLocal(long n) {
    mBytesReadLocal.inc(n);
  }

  /**
   * Increments the counter of bytes read remotelly.
   *
   * @param n the increment
   */
  public void incBytesReadRemote(long n) {
    mBytesReadRemote.inc(n);
  }

  /**
   * Increments the counter of bytes read from UFS.
   *
   * @param n the increment
   */
  public void incBytesReadUfs(long n) {
    mBytesReadUfs.inc(n);
  }

  /**
   * Increments the counter of bytes written locally.
   *
   * @param n the increment
   */
  public void incBytesWrittenLocal(long n) {
    mBytesWrittenLocal.inc(n);
  }

  /**
   * Increments the counter of bytes written remotely.
   *
   * @param n the increment
   */
  public void incBytesWrittenRemote(long n) {
    mBytesWrittenRemote.inc(n);
  }

  /**
   * Increments the counter of bytes written to UFS.
   *
   * @param n the increment
   */
  public void incBytesWrittenUfs(long n) {
    mBytesWrittenUfs.inc(n);
  }

  /**
   * Registers metric gauges.
   *
    * @param blockWorker the block worker handle
   */
  public void registerGauges(final BlockWorker blockWorker) {
    if (mGaugesRegistered) {
      return;
    }
    mMetricRegistry.register(MetricRegistry.name(CAPACITY_TOTAL), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return blockWorker.getStoreMeta().getCapacityBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name(CAPACITY_USED), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return blockWorker.getStoreMeta().getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name(CAPACITY_FREE), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return blockWorker.getStoreMeta().getCapacityBytes()
                - blockWorker.getStoreMeta().getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name(BLOCKS_CACHED), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return blockWorker.getStoreMeta().getNumberOfBlocks();
      }
    });
    mGaugesRegistered = true;
  }
}
