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

package alluxio.metrics;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.metrics.sink.Sink;
import alluxio.metrics.source.Source;
import alluxio.util.network.NetworkAddressUtils;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;
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

  // Supported special instance names.
  public static final String MASTER_INSTANCE = "master";
  public static final String WORKER_INSTANCE = "worker";

  public static final String SINK_REGEX = "^sink\\.(.+)\\.(.+)";
  public static final String SOURCE_REGEX = "^source\\.(.+)\\.(.+)";
  private static final TimeUnit MINIMAL_POLL_UNIT = TimeUnit.SECONDS;
  private static final int MINIMAL_POLL_PERIOD = 1;

  private String mInstance;
  private List<Sink> mSinks = new ArrayList<>();
  private List<Source> mSources = new ArrayList<>();
  private MetricRegistry mMetricRegistry = new MetricRegistry();
  private MetricsConfig mMetricsConfig;
  private boolean mRunning = false;
  private Sink mMetricsServlet;

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
   */
  public MetricsSystem(String instance) {
    mInstance = instance;
    String metricsConfFile = Configuration.get(PropertyKey.METRICS_CONF_FILE);
    mMetricsConfig = new MetricsConfig(metricsConfFile);
  }

  /**
   * Creates a {@code MetricsSystem} using the given {@code MetricsConfig}.
   *
   * @param instance the instance name
   * @param metricsConfig the {@code MetricsConfig} object
   */
  public MetricsSystem(String instance, MetricsConfig metricsConfig) {
    mInstance = instance;
    mMetricsConfig = metricsConfig;
  }

  /**
   * @return the MetricsServlet sink
   */
  public Sink getMetricsServlet() {
    return mMetricsServlet;
  }

  /**
   * Build unique metric registry names. The pattern is [master|worker|client].hostname.sourceName.
   * The hostname is skipped for master.
   *
   * @param instance the instance name
   * @param source the metrics source (e.g. JvmSource, MasterSource)
   * @return the registry name
   */
  public static String buildSourceRegistryName(String instance, Source source) {
    // Do not add hostname to the master metrics.
    if (instance.equals(MASTER_INSTANCE)) {
      return Joiner.on(".").join(instance, source.getName());
    } else {
      return Joiner.on(".").join(instance, NetworkAddressUtils.getLocalHostName().replace('.', ','),
          source.getName());
    }
  }

  /**
   * Registers a {@link Source}.
   *
   * @param source the source to register
   */
  public void registerSource(Source source) {
    mSources.add(source);
    try {
      mMetricRegistry
          .register(buildSourceRegistryName(mInstance, source), source.getMetricRegistry());
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
              (Sink) Class.forName(classPath).getConstructor(Properties.class, MetricRegistry.class)
                  .newInstance(entry.getValue(), mMetricRegistry);
          if (entry.getKey().equals("servlet")) {
            mMetricsServlet = sink;
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

  /**
   * @return the metric registry
   */
  public MetricRegistry getMetricRegistry() {
    return mMetricRegistry;
  }

  /**
   * Util function to remove get the metrics name without instance and host.
   * @param metricsName the long metrics name with instance and host name
   * @return the metrics name without instance and host name
   */
  public static String stripInstanceAndHost(String metricsName) {
    String[] pieces = metricsName.split("\\.");
    if (pieces.length <= 1) {
      throw new IllegalArgumentException("Incorrect metrics name: " + metricsName);
    }

    // Master metrics doesn't have hostname included.
    if (!pieces[0].equals(MASTER_INSTANCE)) {
      pieces[1] = null;
    }
    pieces[0] = null;
    return Joiner.on(".").skipNulls().join(pieces);
  }
}
