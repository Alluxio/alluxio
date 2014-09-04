package tachyon.metrics;

import com.codahale.metrics.MetricRegistry;

public final class Metrics {
  private Metrics() {

  }

  private static final MetricRegistry METRICS = new MetricRegistry();

  public static MetricRegistry metrics() {
    return METRICS;
  }

  public static String name(final Class<?> clazz, final String... args) {
    return MetricRegistry.name(clazz, args);
  }

  public static String name(final String... args) {
    return MetricRegistry.name(new Throwable().getStackTrace()[1].getClassName(), args);
  }
}
