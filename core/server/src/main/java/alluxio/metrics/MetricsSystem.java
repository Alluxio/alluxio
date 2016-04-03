/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.metrics;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.metrics.sink.Sink;
import alluxio.metrics.source.Source;

import com.codahale.metrics.MetricRegistry;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;
/**
 * A MetricsSystem is created by a specific instance(master, worker). It polls the metrics sources
 * periodically and pass the data to the sinks.
 *
 * The syntax of the metrics configuration file is:
 * [instance].[sink|source].[name].[options]=[value]
 */
@NotThreadSafe
public class MetricsSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static final String SINK_REGEX = "^sink\\.(.+)\\.(.+)";
  public static final String SOURCE_REGEX = "^source\\.(.+)\\.(.+)";
  private static final TimeUnit MINIMAL_POLL_UNIT = TimeUnit.SECONDS;
  private static final int MINIMAL_POLL_PERIOD = 1;

  private String mInstance;
  private List<Sink> mSinks = new ArrayList<Sink>();
  private List<Source> mSources = new ArrayList<Source>();
  private MetricRegistry mMetricRegistry = new MetricRegistry();
  private MetricsConfig mMetricsConfig;
  private boolean mRunning = false;
  private Configuration mConfiguration;
  private MetricsServlet mMetricsServlet;

  /**
   * Gets the sinks.
   *
   * @return a list of registered Sinks
   */
  public List<Sink> getSinks() {
    return mSinks;
  }

  /**
   * Gets the sources. Used by unit tests only.
   *
   * @return a list of registered Sources
   */
  public List<Source> getSources() {
    return mSources;
  }

  /**
   * Checks if the poll period is smaller that the minimal poll period which is 1 second.
   *
   * @param pollUnit the polling unit
   * @param pollPeriod the polling period
   * @throws IllegalArgumentException if the polling period is invalid
   */
  public static void checkMinimalPollingPeriod(TimeUnit pollUnit, int pollPeriod)
      throws IllegalArgumentException {
    int period = (int) MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit);
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit
          + " is below than minimal polling period");
    }
  }

  /**
   * Creates a {@code MetricsSystem} using the default metrics config.
   *
   * @param instance the instance name
   * @param configuration the {@link Configuration} instance for configuration properties
   */
  public MetricsSystem(String instance, Configuration configuration) {
    mInstance = instance;
    mConfiguration = configuration;
    String metricsConfFile = null;
    metricsConfFile = mConfiguration.get(Constants.METRICS_CONF_FILE);
    mMetricsConfig = new MetricsConfig(metricsConfFile);
  }

  /**
   * Creates a {@code MetricsSystem} using the given {@code MetricsConfig}.
   *
   * @param instance the instance name
   * @param metricsConfig the {@code MetricsConfig} object
   * @param configuration the {@link Configuration} instance for configuration properties
   */
  public MetricsSystem(String instance, MetricsConfig metricsConfig, Configuration configuration) {
    mInstance = instance;
    mMetricsConfig = metricsConfig;
    mConfiguration = configuration;
  }

  /***
   * Gets the {@link ServletContextHandler} of the metrics servlet.
   *
   * @return the ServletContextHandler if the metrics system is running and the metrics servlet
   *         exists, otherwise null
   */
  public ServletContextHandler getServletHandler() {
    if (mRunning && mMetricsServlet != null) {
      return mMetricsServlet.getHandler();
    }
    return null;
  }

  /**
   * Registers a {@link Source}.
   *
   * @param source the source to register
   */
  public void registerSource(Source source) {
    mSources.add(source);
    try {
      mMetricRegistry.register(source.getName(), source.getMetricRegistry());
    } catch (IllegalArgumentException e) {
      LOG.warn("Metrics already registered. Exception: {}", e.getMessage());
    }
  }

  /**
   * Registers all the sources configured in the metrics config.
   */
  private void registerSources() {
    Properties instConfig = mMetricsConfig.getInstanceProperties(mInstance);
    Map<String, Properties> sourceConfigs = mMetricsConfig.subProperties(instConfig, SOURCE_REGEX);
    for (Map.Entry<String, Properties> entry : sourceConfigs.entrySet()) {
      String classPath = entry.getValue().getProperty("class");
      if (classPath != null) {
        try {
          Source source = (Source) Class.forName(classPath).newInstance();
          registerSource(source);
        } catch (Exception e) {
          LOG.error("Source class {} cannot be instantiated", classPath, e);
        }
      }
    }
  }

  /**
   * Registers all the sinks configured in the metrics config.
   */
  private void registerSinks() {
    Properties instConfig = mMetricsConfig.getInstanceProperties(mInstance);
    Map<String, Properties> sinkConfigs = mMetricsConfig.subProperties(instConfig, SINK_REGEX);
    for (Map.Entry<String, Properties> entry : sinkConfigs.entrySet()) {
      String classPath = entry.getValue().getProperty("class");
      if (classPath != null) {
        try {
          Sink sink =
              (Sink) Class.forName(classPath)
                  .getConstructor(Properties.class, MetricRegistry.class)
                  .newInstance(entry.getValue(), mMetricRegistry);
          if (entry.getKey().equals("servlet")) {
            mMetricsServlet = (MetricsServlet) sink;
          } else {
            mSinks.add(sink);
          }
        } catch (Exception e) {
          LOG.error("Sink class {} cannot be instantiated", classPath, e);
        }
      }
    }
  }

  /**
   * Removes a {@link Source}.
   *
   * @param source the source to remove
   */
  public void removeSource(Source source) {
    mSources.remove(source);
    mMetricRegistry.remove(source.getName());
  }

  /**
   * Reports metrics values to all sinks.
   */
  public void report() {
    for (Sink sink : mSinks) {
      sink.report();
    }
  }

  /**
   * Starts the metrics system.
   */
  public void start() {
    if (!mRunning) {
      registerSources();
      registerSinks();
      for (Sink sink : mSinks) {
        sink.start();
      }
      mRunning = true;
    } else {
      LOG.warn("Attempting to start a MetricsSystem that is already running");
    }
  }

  /**
   * Stops the metrics system.
   */
  public void stop() {
    if (mRunning) {
      for (Sink sink : mSinks) {
        sink.stop();
      }
      mRunning = false;
    } else {
      LOG.warn("Stopping a MetricsSystem that is not running");
    }
  }
}
