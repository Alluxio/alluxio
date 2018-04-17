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
