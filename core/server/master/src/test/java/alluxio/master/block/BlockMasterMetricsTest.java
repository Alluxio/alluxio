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

package alluxio.master.block;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.MasterStorageTierAssoc;
import alluxio.StorageTierAssoc;
import alluxio.master.block.DefaultBlockMaster.Metrics;
import alluxio.master.metastore.BlockStore;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

/**
 * Unit tests for {@link DefaultBlockMaster.Metrics}.
 */
public final class BlockMasterMetricsTest {
  private static final String MEM = Constants.MEDIUM_MEM;
  private static final String HDD = Constants.MEDIUM_HDD;

  private DefaultBlockMaster mBlockMaster;

  @Before
  public void before() throws Exception {
    MetricsSystem.clearAllMetrics();
    mBlockMaster = Mockito.mock(DefaultBlockMaster.class);
    StorageTierAssoc assoc = new MasterStorageTierAssoc(Lists.newArrayList(MEM, HDD));
    when(mBlockMaster.getGlobalStorageTierAssoc()).thenReturn(assoc);
    Metrics.registerGauges(mBlockMaster);
  }

  @Test
  public void testMetricsCapacity() {
    when(mBlockMaster.getCapacityBytes()).thenReturn(1000L);
    assertEquals(1000L, getGauge(MetricKey.CLUSTER_CAPACITY_TOTAL.getName()));
    when(mBlockMaster.getUsedBytes()).thenReturn(200L);
    assertEquals(200L, getGauge(MetricKey.CLUSTER_CAPACITY_USED.getName()));
    assertEquals(800L, getGauge(MetricKey.CLUSTER_CAPACITY_FREE.getName()));
  }

  @Test
  public void testMetricsTierCapacity() {
    when(mBlockMaster.getTotalBytesOnTiers()).thenReturn(ImmutableMap.of(MEM, 1000L, HDD, 2000L));
    when(mBlockMaster.getUsedBytesOnTiers()).thenReturn(ImmutableMap.of(MEM, 100L, HDD, 200L));
    assertEquals(1000L,
        getGauge(MetricKey.CLUSTER_CAPACITY_TOTAL.getName() + MetricInfo.TIER + MEM));
    assertEquals(2000L,
        getGauge(MetricKey.CLUSTER_CAPACITY_TOTAL.getName() + MetricInfo.TIER + HDD));
    assertEquals(100L,
        getGauge(MetricKey.CLUSTER_CAPACITY_USED.getName() + MetricInfo.TIER + MEM));
    assertEquals(200L,
        getGauge(MetricKey.CLUSTER_CAPACITY_USED.getName() + MetricInfo.TIER + HDD));
    assertEquals(900L,
        getGauge(MetricKey.CLUSTER_CAPACITY_FREE.getName() + MetricInfo.TIER + MEM));
    assertEquals(1800L,
        getGauge(MetricKey.CLUSTER_CAPACITY_FREE.getName() + MetricInfo.TIER + HDD));
  }

  @Test
  public void testSize() {
    BlockStore blockStore = Mockito.mock(BlockStore.class);
    when(blockStore.size()).thenReturn(100L);
    Whitebox.setInternalState(mBlockMaster, "mBlockStore", blockStore);
    assertEquals(100L, getGauge(MetricKey.MASTER_UNIQUE_BLOCKS.getName()));
  }

  public void testMetricWorkers() {
    when(mBlockMaster.getWorkerCount()).thenReturn(200);
    assertEquals(200, getGauge(MetricKey.CLUSTER_WORKERS.getName()));
  }

  private Object getGauge(String name) {
    return MetricsSystem.METRIC_REGISTRY.getGauges()
        .get(name).getValue();
  }
}
