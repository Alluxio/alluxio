package alluxio.metrics;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MetricsAggregator {
  String getName();

  List<MetricsFilter> getFilters();

  /**
   * Gets the aggregated value from the filtered metrics.
   *
   * @param map a map of {@link MetricsFilter} to a map of a set of {@link MetricsSystem.Metric}.
   * @return the aggregated value
   */
  Object getValue(Map<MetricsFilter, Set<Metric>> map);
}
