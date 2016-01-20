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

package tachyon.worker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import tachyon.metrics.source.Source;
import tachyon.worker.block.BlockWorker;

/**
 * A WorkerSource collects a Worker's internal state.
 */
public class WorkerSource implements Source {
  private static final String WORKER_SOURCE_NAME = "worker";
  private boolean mGaugesRegistered = false;
  private final MetricRegistry mMetricRegistry = new MetricRegistry();
  private final Counter mBlocksAccessed =
      mMetricRegistry.counter(MetricRegistry.name("BlocksAccessed"));
  private final Counter mBlocksCanceled =
      mMetricRegistry.counter(MetricRegistry.name("BlocksCanceled"));
  private final Counter mBlocksDeleted =
      mMetricRegistry.counter(MetricRegistry.name("BlocksDeleted"));
  private final Counter mBlocksEvicted =
      mMetricRegistry.counter(MetricRegistry.name("BlocksEvicted"));
  private final Counter mBlocksPromoted =
      mMetricRegistry.counter(MetricRegistry.name("BlocksPromoted"));

  // metrics from client
  private final Counter mBlocksReadLocal = mMetricRegistry.counter(MetricRegistry
      .name("BlocksReadLocal"));
  private final Counter mBlocksReadRemote = mMetricRegistry.counter(MetricRegistry
      .name("BlocksReadRemote"));
  private final Counter mBlocksWrittenLocal = mMetricRegistry.counter(MetricRegistry
      .name("BlocksWrittenLocal"));
  private final Counter mBlocksWrittenRemote = mMetricRegistry.counter(MetricRegistry
      .name("BlocksWrittenRemote"));
  private final Counter mBytesReadLocal = mMetricRegistry.counter(MetricRegistry
      .name("BytesReadLocal"));
  private final Counter mBytesReadRemote = mMetricRegistry.counter(MetricRegistry
      .name("BytesReadRemote"));
  private final Counter mBytesReadUfs = mMetricRegistry.counter(MetricRegistry
      .name("BytesReadUfs"));
  private final Counter mBytesWrittenLocal = mMetricRegistry.counter(MetricRegistry
      .name("BytesWrittenLocal"));
  private final Counter mBytesWrittenRemote = mMetricRegistry.counter(MetricRegistry
      .name("BytesWrittenRemote"));
  private final Counter mBytesWrittenUfs = mMetricRegistry.counter(MetricRegistry
      .name("BytesWrittenUfs"));

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
    * @param blockDataManager a block data manager handle
   */
  public void registerGauges(final BlockWorker blockDataManager) {
    if (mGaugesRegistered) {
      return;
    }
    mMetricRegistry.register(MetricRegistry.name("CapacityTotal"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return blockDataManager.getStoreMeta().getCapacityBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("CapacityUsed"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return blockDataManager.getStoreMeta().getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("CapacityFree"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return blockDataManager.getStoreMeta().getCapacityBytes()
                - blockDataManager.getStoreMeta().getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("BlocksCached"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return blockDataManager.getStoreMeta().getNumberOfBlocks();
      }
    });
    mGaugesRegistered = true;
  }
}
