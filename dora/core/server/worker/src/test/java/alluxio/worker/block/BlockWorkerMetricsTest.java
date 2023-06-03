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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.worker.block.DefaultBlockWorker.Metrics;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DefaultBlockWorker.Metrics}.
 */
public final class BlockWorkerMetricsTest {
  private static final String MEM = Constants.MEDIUM_MEM;
  private static final String HDD = Constants.MEDIUM_HDD;

  private BlockWorker mBlockWorker;
  private BlockStoreMeta mBlockStoreMeta;

  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.WORKER_TIERED_STORE_LEVELS, 2);
    Configuration.set(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(0), MEM);
    Configuration.set(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(1), HDD);
    MetricsSystem.clearAllMetrics();
    mBlockWorker = mock(BlockWorker.class);
    mBlockStoreMeta = mock(BlockStoreMeta.class);
    when(mBlockWorker.getStoreMeta()).thenReturn(mBlockStoreMeta);
    when(mBlockWorker.getStoreMetaFull()).thenReturn(mBlockStoreMeta);
    Metrics.registerGauges(mBlockWorker);
  }

  @Test
  public void testMetricsCapacity() throws InterruptedException {
    when(mBlockStoreMeta.getCapacityBytes()).thenReturn(1000L);
    Assert.assertEquals(1000L, getGauge(MetricKey.WORKER_CAPACITY_TOTAL.getName()));
    when(mBlockStoreMeta.getUsedBytes()).thenReturn(200L);
    // sleep 5 seconds because the timeout of this registered is CacheGauge,
    // and it's update interval is 5 seconds
    Thread.sleep(DefaultBlockWorker.CACHEGAUGE_UPDATE_INTERVAL);
    Assert.assertEquals(200L, getGauge(MetricKey.WORKER_CAPACITY_USED.getName()));
    Assert.assertEquals(800L, getGauge(MetricKey.WORKER_CAPACITY_FREE.getName()));
  }

  @Test
  public void testMetricsTierCapacity() throws InterruptedException {
    when(mBlockStoreMeta.getCapacityBytesOnTiers())
        .thenReturn(ImmutableMap.of(MEM, 1000L, HDD, 2000L));
    when(mBlockStoreMeta.getUsedBytesOnTiers()).thenReturn(ImmutableMap.of(MEM, 100L, HDD, 200L));
    Thread.sleep(DefaultBlockWorker.CACHEGAUGE_UPDATE_INTERVAL);
    assertEquals(1000L,
        getGauge(MetricKey.WORKER_CAPACITY_TOTAL.getName() + MetricInfo.TIER + MEM));
    assertEquals(2000L,
        getGauge(MetricKey.WORKER_CAPACITY_TOTAL.getName() + MetricInfo.TIER + HDD));
    assertEquals(100L, getGauge(MetricKey.WORKER_CAPACITY_USED.getName() + MetricInfo.TIER + MEM));
    assertEquals(200L, getGauge(MetricKey.WORKER_CAPACITY_USED.getName() + MetricInfo.TIER + HDD));
    assertEquals(900L, getGauge(MetricKey.WORKER_CAPACITY_FREE.getName() + MetricInfo.TIER + MEM));
    assertEquals(1800L, getGauge(MetricKey.WORKER_CAPACITY_FREE.getName() + MetricInfo.TIER + HDD));
  }

  @Test
  public void testMetricBocksCached() {
    when(mBlockStoreMeta.getNumberOfBlocks()).thenReturn(200);
    Assert.assertEquals(200, getGauge(MetricKey.WORKER_BLOCKS_CACHED.getName()));
  }

  private Object getGauge(String name) {
    return MetricsSystem.METRIC_REGISTRY.getGauges()
        .get(MetricsSystem.getMetricName(name)).getValue();
  }
}
