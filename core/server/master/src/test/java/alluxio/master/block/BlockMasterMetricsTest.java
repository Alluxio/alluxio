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

import static org.mockito.Mockito.when;

import alluxio.master.block.DefaultBlockMaster.Metrics;
import alluxio.metrics.MetricsSystem;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultBlockMaster.Metrics}.
 */
public final class BlockMasterMetricsTest {
  private BlockMaster mBlockMaster;

  @Before
  public void before() throws Exception {
    MetricsSystem.clearAllMetrics();
    mBlockMaster = Mockito.mock(BlockMaster.class);
    Metrics.registerGauges(mBlockMaster);
  }

  @Test
  public void testMetricsCapacity() {
    when(mBlockMaster.getCapacityBytes()).thenReturn(1000L);
    Assert.assertEquals(1000L, getGauge(Metrics.CAPACITY_TOTAL));
    when(mBlockMaster.getUsedBytes()).thenReturn(200L);
    Assert.assertEquals(200L, getGauge(Metrics.CAPACITY_USED));
    Assert.assertEquals(800L, getGauge(Metrics.CAPACITY_FREE));
  }

  public void testMetricWorkers() {
    when(mBlockMaster.getWorkerCount()).thenReturn(200);
    Assert.assertEquals(200, getGauge(Metrics.WORKERS));
  }

  private Object getGauge(String name) {
    return MetricsSystem.METRIC_REGISTRY.getGauges().get(MetricsSystem.getMasterMetricName(name))
        .getValue();
  }
}
