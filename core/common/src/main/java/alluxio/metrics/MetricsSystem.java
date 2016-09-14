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
import alluxio.collections.ConcurrentHashSet;
import alluxio.metrics.sink.Sink;
import alluxio.util.network.NetworkAddressUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A MetricsSystem is created by a specific instance(master, worker). It polls the metrics sources
 * periodically and pass the data to the sinks.
 *
 * The syntax of the metrics configuration file is:
 * [instance].[sink|source].[name].[options]=[value]
 */
@ThreadSafe
public class MetricsSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // Supported special instance names.
  public static final String MASTER_INSTANCE = "master";
  public static final String WORKER_INSTANCE = "worker";
  public static final String CLIENT_INSTANCE = "client";

  public static final MetricRegistry METRIC_REGISTRY = initMetricRegistry();

  private static final ReentrantLock mSinksLock = new ReentrantLock();
  @GuardedBy("mSinksLock")
  private static List<Sink> mSinks;

  public static final String SINK_REGEX = "^sink\\.(.+)\\.(.+)";
  private static final TimeUnit MINIMAL_POLL_UNIT = TimeUnit.SECONDS;
  private static final int MINIMAL_POLL_PERIOD = 1;

  /**
   * All the gauges registered.
   */
  private static HashSet<String> mGauges = new HashSet<>();

  /**
   * Start sinks specified in the configuration. This is an no-op if the sinks have already been
   * started.
   * Note: This has to be called after Alluxio configuration is initialized.
   */
  public static void startSinks() {
    String metricsConfFile = Configuration.get(PropertyKey.METRICS_CONF_FILE);
    if (metricsConfFile.isEmpty()) {
      LOG.info("Metrics is not enabled.");
      return;
    }
    MetricsConfig config = new MetricsConfig(metricsConfFile);
    startSinksFromConfig(config);
  }

  /**
   * Start sinks from a given metrics configuration. This is made public for unit test.
   *
   * @param config the metrics config
   */
  public static void startSinksFromConfig(MetricsConfig config) {
    mSinksLock.lock();
    if (mSinks != null) {
      LOG.warn("Sinks have already been started.");
      mSinksLock.unlock();
      return;
    }

    Map<String, Properties> sinkConfigs =
        MetricsConfig.subProperties(config.getProperties(), SINK_REGEX);
    for (Map.Entry<String, Properties> entry : sinkConfigs.entrySet()) {
      String classPath = entry.getValue().getProperty("class");
      if (classPath != null) {
        try {
          Sink sink =
              (Sink) Class.forName(classPath).getConstructor(Properties.class, MetricRegistry.class)
                  .newInstance(entry.getValue(), METRIC_REGISTRY);
          mSinks.add(sink);
        } catch (Exception e) {
          LOG.error("Sink class {} cannot be instantiated", classPath, e);
        }
      }
    }

    mSinksLock.unlock();
  }

  /**
   * Stop all the sinks.
   */
  public static void stopSinks() {
    mSinksLock.lock();
    if (mSinks != null) {
      for (Sink sink : mSinks) {
        sink.stop();
      }
    }
    mSinks = null;
    mSinksLock.unlock();
  }

  /**
   * @return the number of sinks started
   */
  public static int getNumSinks() {
    int sz = 0;
    mSinksLock.lock();
    if (mSinks != null) {
      sz = mSinks.size();
    }
    mSinksLock.unlock();
    return sz;
  }

  /**
   * Build metric registry names for master instance. The pattern is instance.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  public static String getMasterMetricName(String name) {
    return Joiner.on(".").join(MASTER_INSTANCE, name);
  }

  /**
   * Build metric registry name for worker instance. The pattern is instance.uniqueId.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  public static String getWorkerMetricName(String name) {
    return getMetricNameWithUniqueId(WORKER_INSTANCE, name);
  }

  /**
   * Build metric registry name for client instance. The pattern is instance.uniqueId.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  public static String getClientMetricName(String name) {
    return getMetricNameWithUniqueId(CLIENT_INSTANCE, name);
  }

  /**
   * Build unique metric registry names with unique ID (set to host name). The pattern is
   * instance.hostname.metricName.
   *
   * @param instance the instance name
   * @param name the metric name
   * @return the metric registry name
   */
  public static String getMetricNameWithUniqueId(String instance, String name) {
    return Joiner.on(".")
        .join(instance, NetworkAddressUtils.getLocalHostName().replace('.', '_'), name);
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
   * Util function to remove get the metrics name without instance and host.
   * @param metricsName the long metrics name with instance and host name
   * @return the metrics name without instance and host name
   */
  public static String stripInstanceAndHost(String metricsName) {
    String[] pieces = metricsName.split("\\.");
    Preconditions.checkArgument(pieces.length > 1, "Incorrect metrics name: " + metricsName);

    // Master metrics doesn't have hostname included.
    if (!pieces[0].equals(MASTER_INSTANCE)) {
      pieces[1] = null;
    }
    pieces[0] = null;
    return Joiner.on(".").skipNulls().join(pieces);
  }

  // Some helper functions.

  /**
   * @param name the metric name
   * @return the timer
   */
  public static Timer masterTimer(String name) {
    return METRIC_REGISTRY.timer(getMasterMetricName(name));
  }
  /**
   * @param name the metric name
   * @return the counter
   */
  public static Counter masterCounter(String name) {
    return METRIC_REGISTRY.counter((getMasterMetricName(name)));
  }

  /**
   * @param name the metric name
   * @return the timer
   */
  public static Timer workerTimer(String name) {
    return METRIC_REGISTRY.timer(getWorkerMetricName(name));
  }
  /**
   * @param name the metric name
   * @return the counter
   */
  public static Counter workerCounter(String name) {
    return METRIC_REGISTRY.counter((getWorkerMetricName(name)));
  }

  /**
   * @param name the metric name
   * @return the timer
   */
  public static Timer clientTimer(String name) {
    return METRIC_REGISTRY.timer(getClientMetricName(name));
  }
  /**
   * @param name the metric name
   * @return the counter
   */
  public static Counter clientCounter(String name) {
    return METRIC_REGISTRY.counter(getClientMetricName(name));
  }

  /**
   * Initialize the metric registry.
   *
   * @return the metric registry
   */
  private static MetricRegistry initMetricRegistry() {
    MetricRegistry metricRegistry = new MetricRegistry();
    metricRegistry.registerAll(new GarbageCollectorMetricSet());
    metricRegistry.registerAll(new MemoryUsageGaugeSet());
    return metricRegistry;
  }

  /**
   * Register a gauge if it has not been registered. Register all the gauges with this
   * function to avoid register the same one multiple times.
   *
   * @param name the gauge name
   * @param metric the gauge
   * @param <T> the type
   */
  public synchronized static <T> void registerGaugeIfAbsent(String name, Gauge<T> metric) {
    if (!METRIC_REGISTRY.getGauges().containsKey(name)) {
      mGauges.add(name);
      METRIC_REGISTRY.register(name, metric);
    }
  }

  /**
   * Disallow any explicit initialization.
   */
  private MetricsSystem() {
  }
}
