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

import static org.mockito.Mockito.when;

import alluxio.metrics.MetricsSystem;
import alluxio.worker.block.DefaultBlockWorker.Metrics;

import org.junit.Assert;
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
    when(mBlockStoreMeta.getCapacityBytes()).thenReturn(1000L);
    Assert.assertEquals(1000L, getGauge(Metrics.CAPACITY_TOTAL));
    when(mBlockStoreMeta.getUsedBytes()).thenReturn(200L);
    Assert.assertEquals(200L, getGauge(Metrics.CAPACITY_USED));
    Assert.assertEquals(800L, getGauge(Metrics.CAPACITY_FREE));
  }

  public void testMetricBocksCached() {
    when(mBlockStoreMeta.getNumberOfBlocks()).thenReturn(200);
    Assert.assertEquals(200, getGauge(Metrics.BLOCKS_CACHED));
  }

  private Object getGauge(String name) {
    return MetricsSystem.METRIC_REGISTRY.getGauges().get(MetricsSystem.getWorkerMetricName(name))
        .getValue();
  }
}
