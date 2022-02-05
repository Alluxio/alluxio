package alluxio.client.metrics;

/**
 * This represents the type of how to collect and store the scoped metrics breakdown
 */
public enum ScopedMetricsType {
  /**
   * JMX or CSV based metrics that utilize the metrics implementation of Alluxio System
   */
  ALLUXIO_SYSTEM,
  /**
   * Hashmap based scoped metrics
   */
  IN_MEMORY,
  /**
   * No metrics collected
   */
  NO_OP
}
