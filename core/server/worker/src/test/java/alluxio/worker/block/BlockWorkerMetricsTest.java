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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import alluxio.Constants;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.worker.block.DefaultBlockWorker.Metrics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DefaultBlockWorker.Metrics}.
 */
public final class BlockWorkerMetricsTest {

  private BlockWorker mBlockWorker;
  private BlockStoreMeta mBlockStoreMeta;

  @Before
  public void before() throws Exception {
    MetricsSystem.clearAllMetrics();
    mBlockWorker = mock(BlockWorker.class);
    mBlockStoreMeta = mock(BlockStoreMeta.class);
    when(mBlockWorker.getStoreMeta()).thenReturn(mBlockStoreMeta);
    StorageTierAssoc assoc = new WorkerStorageTierAssoc(Lists.newArrayList(Constants.MEDIUM_MEM, Constants.MEDIUM_HDD));
    when(mBlockStoreMeta.getStorageTierAssoc()).thenReturn(assoc);
    Metrics.registerGauges(mBlockWorker);
  }

  @Test
  public void testMetricsCapacity() {
    when(mBlockStoreMeta.getCapacityBytes()).thenReturn(1000L);
    Assert.assertEquals(1000L, getGauge(MetricKey.WORKER_CAPACITY_TOTAL.getName()));
    when(mBlockStoreMeta.getUsedBytes()).thenReturn(200L);
    Assert.assertEquals(200L, getGauge(MetricKey.WORKER_CAPACITY_USED.getName()));
    Assert.assertEquals(800L, getGauge(MetricKey.WORKER_CAPACITY_FREE.getName()));
  }

  @Test
  public void testMetricsTierCapacity() {
    when(mBlockStoreMeta.getCapacityBytesOnTiers())
        .thenReturn(ImmutableMap.of(Constants.MEDIUM_MEM, 1000L, Constants.MEDIUM_HDD, 2000L));
    when(mBlockStoreMeta.getUsedBytesOnTiers()).thenReturn(ImmutableMap.of(Constants.MEDIUM_MEM, 100L, Constants.MEDIUM_HDD, 200L));
    assertEquals(1000L,
        getGauge(MetricKey.WORKER_CAPACITY_TOTAL.getName() + MetricInfo.TIER + Constants.MEDIUM_MEM));
    assertEquals(2000L,
        getGauge(MetricKey.WORKER_CAPACITY_TOTAL.getName() + MetricInfo.TIER + Constants.MEDIUM_HDD));
    assertEquals(100L, getGauge(MetricKey.WORKER_CAPACITY_USED.getName() + MetricInfo.TIER + Constants.MEDIUM_MEM));
    assertEquals(200L, getGauge(MetricKey.WORKER_CAPACITY_USED.getName() + MetricInfo.TIER + Constants.MEDIUM_HDD));
    assertEquals(900L, getGauge(MetricKey.WORKER_CAPACITY_FREE.getName() + MetricInfo.TIER + Constants.MEDIUM_MEM));
    assertEquals(1800L, getGauge(MetricKey.WORKER_CAPACITY_FREE.getName() + MetricInfo.TIER + Constants.MEDIUM_HDD));
  }

  public void testMetricBocksCached() {
    when(mBlockStoreMeta.getNumberOfBlocks()).thenReturn(200);
    Assert.assertEquals(200, getGauge(MetricKey.WORKER_BLOCKS_CACHED.getName()));
  }

  private Object getGauge(String name) {
    return MetricsSystem.METRIC_REGISTRY.getGauges()
        .get(MetricsSystem.getMetricName(name)).getValue();
  }
}
