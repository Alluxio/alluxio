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

/**
 * A WorkerSource collects a Worker's internal state.
 */
public class WorkerSource implements Source {
  private MetricRegistry mMetricRegistry = new MetricRegistry();
  private final Counter mBlocksAccessed = mMetricRegistry.counter(MetricRegistry
          .name("BlocksAccessed"));
  private final Counter mBlocksCanceled = mMetricRegistry.counter(MetricRegistry
          .name("BlocksCanceled"));
  private final Counter mBlocksDeleted = mMetricRegistry.counter(MetricRegistry
          .name("BlocksDeleted"));
  private final Counter mBlocksEvicted = mMetricRegistry.counter(MetricRegistry
          .name("BlocksEvicted"));
  private final Counter mBlocksPromoted = mMetricRegistry.counter(MetricRegistry
          .name("BlocksPromoted"));

  public WorkerSource(final WorkerStorage workerStorage) {
    mMetricRegistry.register(MetricRegistry.name("CapacityTotal"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return workerStorage.getCapacityBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("CapacityUsed"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return workerStorage.getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("CapacityFree"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return workerStorage.getCapacityBytes() - workerStorage.getUsedBytes();
      }
    });

    mMetricRegistry.register(MetricRegistry.name("BlocksCached"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return workerStorage.getNumberOfBlocks();
      }
    });
  }

  @Override
  public String getName() {
    return "worker";
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


}
