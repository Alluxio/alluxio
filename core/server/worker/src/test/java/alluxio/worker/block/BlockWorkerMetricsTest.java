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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import alluxio.metrics.MetricsSystem;
import alluxio.worker.block.DefaultBlockWorker.Metrics;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultBlockWorker.Metrics}.
 */
public final class BlockWorkerMetricsTest {
  private BlockWorker mBlockWorker;
  private BlockStoreMeta mBlockStoreMeta;

  @Before
  public void before() throws Exception {
    MetricsSystem.clearAllMetrics();
    mBlockWorker = Mockito.mock(BlockWorker.class);
    mBlockStoreMeta = Mockito.mock(BlockStoreMeta.class);
    Mockito.when(mBlockWorker.getStoreMeta()).thenReturn(mBlockStoreMeta);
    Metrics.registerGauges(mBlockWorker);
  }

  @Test
  public void testMetricsCapacity() {
    getGauge(Metrics.CAPACITY_TOTAL);
    verify(mBlockStoreMeta).getCapacityBytes();
    getGauge(Metrics.CAPACITY_USED);
    verify(mBlockStoreMeta).getUsedBytes();
    getGauge(Metrics.CAPACITY_FREE);
    verify(mBlockStoreMeta, times(2)).getCapacityBytes();
    verify(mBlockStoreMeta, times(2)).getUsedBytes();
  }

  public void testMetricBocksCached() {
    getGauge(Metrics.BLOCKS_CACHED);
    verify(mBlockStoreMeta).getNumberOfBlocks();
  }

  void getGauge(String name) {
    MetricsSystem.METRIC_REGISTRY.getGauges().get(MetricsSystem.getWorkerMetricName(name))
        .getValue();
  }
}
