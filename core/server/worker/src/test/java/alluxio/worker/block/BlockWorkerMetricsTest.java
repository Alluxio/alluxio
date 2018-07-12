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

import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.metrics.MetricsSystem;
import alluxio.worker.block.DefaultBlockWorker.Metrics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultBlockWorker.Metrics}.
 */
public final class BlockWorkerMetricsTest {
  private static final String MEM = "MEM";
  private static final String HDD = "HDD";

  private BlockWorker mBlockWorker;
  private BlockStoreMeta mBlockStoreMeta;

  @Before
  public void before() throws Exception {
    MetricsSystem.clearAllMetrics();
    mBlockWorker = Mockito.mock(BlockWorker.class);
    mBlockStoreMeta = Mockito.mock(BlockStoreMeta.class);
    Mockito.when(mBlockWorker.getStoreMeta()).thenReturn(mBlockStoreMeta);
    StorageTierAssoc assoc = new WorkerStorageTierAssoc(Lists.newArrayList(MEM, HDD));
    when(mBlockStoreMeta.getStorageTierAssoc()).thenReturn(assoc);
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

  @Test
  public void testMetricsTierCapacity() {
    when(mBlockStoreMeta.getCapacityBytesOnTiers())
        .thenReturn(ImmutableMap.of(MEM, 1000L, HDD, 2000L));
    when(mBlockStoreMeta.getUsedBytesOnTiers()).thenReturn(ImmutableMap.of(MEM, 100L, HDD, 200L));
    assertEquals(1000L, getGauge(Metrics.CAPACITY_TOTAL + Metrics.TIER + MEM));
    assertEquals(2000L, getGauge(Metrics.CAPACITY_TOTAL + Metrics.TIER + HDD));
    assertEquals(100L, getGauge(Metrics.CAPACITY_USED + Metrics.TIER + MEM));
    assertEquals(200L, getGauge(Metrics.CAPACITY_USED + Metrics.TIER + HDD));
    assertEquals(900L, getGauge(Metrics.CAPACITY_FREE + Metrics.TIER + MEM));
    assertEquals(1800L, getGauge(Metrics.CAPACITY_FREE + Metrics.TIER + HDD));
  }

  public void testMetricBocksCached() {
    when(mBlockStoreMeta.getNumberOfBlocks()).thenReturn(200);
    Assert.assertEquals(200, getGauge(Metrics.BLOCKS_CACHED));
  }

  private Object getGauge(String name) {
    return MetricsSystem.METRIC_REGISTRY.getGauges().get(MetricsSystem.getMetricName(name))
        .getValue();
  }
}
