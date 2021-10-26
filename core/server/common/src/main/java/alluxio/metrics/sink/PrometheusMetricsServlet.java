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
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A sink that exposes metrics data in prometheus format by HTTP.
 * See https://github.com/prometheus/client_java/issues/101
 */
@NotThreadSafe
public class PrometheusMetricsServlet implements Sink {

  private static final String SERVLET_PATH = "/metrics/prometheus";

  private CollectorRegistry mCollectorRegistry;

  /**
   * Creates a new {@link alluxio.metrics.sink.PrometheusMetricsServlet} with a
   * {@link MetricRegistry}.
   *
   * @param registry the metric registry to register
   */
  public PrometheusMetricsServlet(MetricRegistry registry) {
    mCollectorRegistry = CollectorRegistry.defaultRegistry;
    mCollectorRegistry.register(new DropwizardExports(registry));
  }

  /**
   * Creates a new {@link alluxio.metrics.sink.PrometheusMetricsServlet} with a
   * {@link MetricRegistry}.
   *
   * @param properties the properties, not used for now
   * @param registry the metric registry to register
   */
  public PrometheusMetricsServlet(Properties properties, MetricRegistry registry) {
    this(registry);
  }

  /**
   * Gets the {@link ServletContextHandler} of the metrics servlet.
   *
   * @return the {@link ServletContextHandler} object
   */
  public ServletContextHandler getHandler() {
    ServletContextHandler contextHandler = new ServletContextHandler();
    contextHandler.setContextPath(SERVLET_PATH);
    contextHandler.addServlet(new ServletHolder(new MetricsServlet(mCollectorRegistry)), "/");
    return contextHandler;
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void report() {
  }
}

