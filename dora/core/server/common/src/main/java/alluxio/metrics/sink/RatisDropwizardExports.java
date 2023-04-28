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

package alluxio.metrics.sink;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.RatisMetricRegistry;

import java.util.Map;

/**
 * Collect Dropwizard metrics, but rename ratis specific metrics.
 */
public class RatisDropwizardExports extends DropwizardExports {

  /**
   * Creates a new DropwizardExports with a {@link DefaultSampleBuilder}.
   *
   * @param registry a metric registry to export in prometheus
   */
  public RatisDropwizardExports(MetricRegistry registry) {
    super(registry, new RatisNameRewriteSampleBuilder());
  }

  /**
   * Register ratis metric to metricRegistry.
   * @param ratisMetricsMap a map to store the registered metrics
   */
  public static void registerRatisMetricReporters(
      Map<String, RatisDropwizardExports> ratisMetricsMap) {
    MetricRegistries.global().addReporterRegistration(
        r1 -> registerDropwizard(r1, ratisMetricsMap),
        r2 -> deregisterDropwizard(r2, ratisMetricsMap));
  }

  private static void registerDropwizard(RatisMetricRegistry registry,
      Map<String, RatisDropwizardExports> ratisMetricsMap) {
    RatisDropwizardExports rde = new RatisDropwizardExports(
        registry.getDropWizardMetricRegistry());
    CollectorRegistry.defaultRegistry.register(rde);
    String name = registry.getMetricRegistryInfo().getName();
    ratisMetricsMap.putIfAbsent(name, rde);
  }

  private static void deregisterDropwizard(RatisMetricRegistry registry,
      Map<String, RatisDropwizardExports> ratisMetricsMap) {
    String name = registry.getMetricRegistryInfo().getName();
    Collector c = ratisMetricsMap.remove(name);
    if (c != null) {
      CollectorRegistry.defaultRegistry.unregister(c);
    }
  }
}
