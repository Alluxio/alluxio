package alluxio.metrics.aggregator;

import alluxio.metrics.Metric;
import alluxio.metrics.MetricsAggregator;
import alluxio.metrics.MetricsFilter;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BytesReadAlluxio implements MetricsAggregator {
  public static final String NAME ="BytesReadAlluxio";
  public static final MetricsFilter BYTES_READ_ALLUXIO_FILTER=new MetricsFilter(MetricsSystem.WORKER_INSTANCE, WorkerMetrics.BYTES_READ_ALLUXIO);

  @Override
  public String getName() {
    return MetricsSystem.getClientMetricName(NAME);
  }

  @Override
  public List<MetricsFilter> getFilters() {
    return Lists.newArrayList(BYTES_READ_ALLUXIO_FILTER);
  }

  @Override
  public Object getValue(Map<MetricsFilter, Set<Metric>> map) {
    long value = 0;
    for (Metric metric : map.get(BYTES_READ_ALLUXIO_FILTER)) {
      value += (long) metric.getValue();
    }
    return value;
  }

}
