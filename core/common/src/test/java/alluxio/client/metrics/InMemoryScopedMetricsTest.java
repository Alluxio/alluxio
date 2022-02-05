package alluxio.client.metrics;

public class InMemoryScopedMetricsTest extends BaseScopedMetricsTest{

  @Override
  protected ScopedMetrics createMetrics() {
    return new InMemoryScopedMetrics();
  }
}
