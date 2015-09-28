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
import tachyon.worker.block.BlockDataManager;

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

  public void incBlocksAccessed() {
    mBlocksAccessed.inc();
  }

  public void incBlocksCanceled() {
    mBlocksCanceled.inc();
  }

  public void incBlocksDeleted() {
    mBlocksDeleted.inc();
  }

  public void incBlocksEvicted() {
    mBlocksEvicted.inc();
  }

  public void incBlocksPromoted() {
    mBlocksPromoted.inc();
  }

  public void incBlocksReadLocal(long n) {
    mBlocksReadLocal.inc(n);
  }

  public void incBlocksReadRemote(long n) {
    mBlocksReadRemote.inc(n);
  }

  public void incBlocksWrittenLocal(long n) {
    mBlocksWrittenLocal.inc(n);
  }

  public void incBlocksWrittenRemote(long n) {
    mBlocksWrittenRemote.inc(n);
  }

  public void incBytesReadLocal(long n) {
    mBytesReadLocal.inc(n);
  }

  public void incBytesReadRemote(long n) {
    mBytesReadRemote.inc(n);
  }

  public void incBytesReadUfs(long n) {
    mBytesReadUfs.inc(n);
  }

  public void incBytesWrittenLocal(long n) {
    mBytesWrittenLocal.inc(n);
  }

  public void incBytesWrittenRemote(long n) {
    mBytesWrittenRemote.inc(n);
  }

  public void incBytesWrittenUfs(long n) {
    mBytesWrittenUfs.inc(n);
  }

  public void registerGauges(final BlockDataManager blockDataManager) {
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
