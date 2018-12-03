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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.metrics.sink.Sink;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.JvmAttributeGaugeSet;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A MetricsSystem is created by a specific instance(master, worker). It polls the metrics sources
 * periodically and pass the data to the sinks.
 *
 * The syntax of the metrics configuration file is: sink.[name].[options]=[value]
 */
@ThreadSafe
public final class MetricsSystem {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsSystem.class);

  private static final ConcurrentHashMap<String, String> CACHED_METRICS = new ConcurrentHashMap<>();
  private static String mAppId;

  /**
   * An enum of supported instance type.
   */
  public enum InstanceType {
    JOB_MASTER("JobMaster"),
    JOB_WORKER("JobWorker"),
    MASTER("Master"),
    WORKER("Worker"),
    CLUSTER("Cluster"),
    CLIENT("Client"),
    PROXY("Proxy");

    private String mValue;

    /**
     * Creates the instance type with value.
     *
     * @param value value of the instance type
     */
    InstanceType(String value) {
      mValue = value;
    }

    @Override
    public String toString() {
      return mValue;
    }

    /**
     * Creates an instance type from the string. This method is case insensitive.
     *
     * @param text the instance type in string
     * @return the created instance
     */
    public static InstanceType fromString(String text) {
      for (InstanceType type : InstanceType.values()) {
        if (type.toString().equalsIgnoreCase(text)) {
          return type;
        }
      }
      throw new IllegalArgumentException("No constant with text " + text + " found");
    }
  }

  // Supported special instance names.
  public static final String CLUSTER = "cluster";

  public static final MetricRegistry METRIC_REGISTRY;

  static {
    METRIC_REGISTRY = new MetricRegistry();
    METRIC_REGISTRY.registerAll(new JvmAttributeGaugeSet());
    METRIC_REGISTRY.registerAll(new GarbageCollectorMetricSet());
    METRIC_REGISTRY.registerAll(new MemoryUsageGaugeSet());
  }

  @GuardedBy("MetricsSystem")
  private static List<Sink> sSinks;

  public static final String SINK_REGEX = "^sink\\.(.+)\\.(.+)";
  private static final TimeUnit MINIMAL_POLL_UNIT = TimeUnit.SECONDS;
  private static final int MINIMAL_POLL_PERIOD = 1;

  /**
   * Starts sinks specified in the configuration. This is an no-op if the sinks have already been
   * started. Note: This has to be called after Alluxio configuration is initialized.
   */
  public static void startSinks() {
    synchronized (MetricsSystem.class) {
      if (sSinks != null) {
        LOG.info("Sinks have already been started.");
        return;
      }
    }
    String metricsConfFile = Configuration.get(PropertyKey.METRICS_CONF_FILE);
    if (metricsConfFile.isEmpty()) {
      LOG.info("Metrics is not enabled.");
      return;
    }
    MetricsConfig config = new MetricsConfig(metricsConfFile);
    startSinksFromConfig(config);
  }

  /**
   * Starts sinks from a given metrics configuration. This is made public for unit test.
   *
   * @param config the metrics config
   */
  public static synchronized void startSinksFromConfig(MetricsConfig config) {
    if (sSinks != null) {
      LOG.info("Sinks have already been started.");
      return;
    }
    LOG.info("Starting sinks with config: {}.", config);
    sSinks = new ArrayList<>();
    Map<String, Properties> sinkConfigs =
        MetricsConfig.subProperties(config.getProperties(), SINK_REGEX);
    for (Map.Entry<String, Properties> entry : sinkConfigs.entrySet()) {
      String classPath = entry.getValue().getProperty("class");
      if (classPath != null) {
        LOG.info("Starting sink {}.", classPath);
        try {
          Sink sink =
              (Sink) Class.forName(classPath).getConstructor(Properties.class, MetricRegistry.class)
                  .newInstance(entry.getValue(), METRIC_REGISTRY);
          sink.start();
          sSinks.add(sink);
        } catch (Exception e) {
          LOG.error("Sink class {} cannot be instantiated", classPath, e);
        }
      }
    }
  }

  /**
   * Stops all the sinks.
   */
  public static synchronized void stopSinks() {
    if (sSinks != null) {
      for (Sink sink : sSinks) {
        sink.stop();
      }
    }
    sSinks = null;
  }

  /**
   * @return the number of sinks started
   */
  public static synchronized int getNumSinks() {
    int sz = 0;
    if (sSinks != null) {
      sz = sSinks.size();
    }
    return sz;
  }

  /**
   * Converts a simple string to a qualified metric name based on the process type.
   *
   * @param name the name of the metric
   * @return the metric with instance and id tags
   */
  public static String getMetricName(String name) {
    switch (CommonUtils.PROCESS_TYPE.get()) {
      case CLIENT:
        return getClientMetricName(name);
      case MASTER:
        return getMasterMetricName(name);
      case PROXY:
        return getProxyMetricName(name);
      case WORKER:
        return getWorkerMetricName(name);
      case JOB_MASTER:
        return getJobMasterMetricName(name);
      case JOB_WORKER:
        return getJobWorkerMetricName(name);
      default:
        throw new IllegalStateException("Unknown process type");
    }
  }

  /**
   * Builds metric registry names for master instance. The pattern is instance.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  private static String getMasterMetricName(String name) {
    String result = CACHED_METRICS.get(name);
    if (result != null) {
      return result;
    }
    return CACHED_METRICS.computeIfAbsent(name, n -> InstanceType.MASTER.toString() + "." + name);
  }

  /**
   * Builds metric registry name for worker instance. The pattern is instance.uniqueId.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  private static String getWorkerMetricName(String name) {
    String result = CACHED_METRICS.get(name);
    if (result != null) {
      return result;
    }
    return CACHED_METRICS.computeIfAbsent(name,
        n -> getMetricNameWithUniqueId(InstanceType.WORKER, name));
  }

  /**
   * Builds metric registry name for client instance. The pattern is instance.uniqueId.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  private static String getClientMetricName(String name) {
    String result = CACHED_METRICS.get(name);
    if (result != null) {
      return result;
    }
    return CACHED_METRICS.computeIfAbsent(name,
        n -> getMetricNameWithUniqueId(InstanceType.CLIENT, name));
  }

  /**
   * Builds metric registry name for a proxy instance. The pattern is instance.uniqueId.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  private static String getProxyMetricName(String name) {
    return getMetricNameWithUniqueId(InstanceType.PROXY, name);
  }

  /**
   * Builds metric registry names for cluster. The pattern is cluster.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  public static String getClusterMetricName(String name) {
    return Joiner.on(".").join(CLUSTER, name);
  }

  /**
   * Builds metric registry names for the job master instance. The pattern is instance.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  private static String getJobMasterMetricName(String name) {
    return Joiner.on(".").join(InstanceType.JOB_MASTER, name);
  }

  /**
   * Builds metric registry name for job worker instance. The pattern is
   * instance.uniqueId.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  public static String getJobWorkerMetricName(String name) {
    return getMetricNameWithUniqueId(InstanceType.JOB_WORKER, name);
  }

  /**
   * Builds unique metric registry names with unique ID (set to host name). The pattern is
   * instance.hostname.metricName.
   *
   * @param instance the instance name
   * @param name the metric name
   * @return the metric registry name
   */
  private static String getMetricNameWithUniqueId(InstanceType instance, String name) {
    return instance + "." + NetworkAddressUtils.getLocalHostMetricName() + "." + name;
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
    Preconditions.checkArgument(period >= MINIMAL_POLL_PERIOD,
        "Polling period %d %d is below the minimal polling period", pollPeriod, pollUnit);
  }

  /**
   * Removes the instance and host from the given metric name, returning the result.
   *
   * @param metricsName the long metrics name with instance and host name
   * @return the metrics name without instance and host name
   */
  public static String stripInstanceAndHost(String metricsName) {
    String[] pieces = metricsName.split("\\.");
    Preconditions.checkArgument(pieces.length > 1, "Incorrect metrics name: %s.", metricsName);

    // Master metrics doesn't have hostname included.
    if (!pieces[0].equals(MetricsSystem.InstanceType.MASTER.toString())) {
      pieces[1] = null;
    }
    pieces[0] = null;
    return Joiner.on(".").skipNulls().join(pieces);
  }

  /**
   * Escapes a URI, replacing "/" and "." with "_" so that when the URI is used in a metric name,
   * the "/" and "." won't be interpreted as path separators.
   *
   * @param uri the URI to escape
   * @return the string representing the escaped URI
   */
  public static String escape(AlluxioURI uri) {
    return uri.toString().replace("/", "_").replace(".", "_");
  }

  // Some helper functions.

  /**
   * @param name the name of the metric
   * @return a counter object with the qualified metric name
   */
  public static Counter counter(String name) {
    return METRIC_REGISTRY.counter(getMetricName(name));
  }

  /**
   * @param name the name of the metric
   * @return a meter object with the qualified metric name
   */
  public static Meter meter(String name) {
    return METRIC_REGISTRY.meter(getMetricName(name));
  }

  /**
   * @param name the name of the metric
   * @return a timer object with the qualified metric name
   */
  public static Timer timer(String name) {
    return METRIC_REGISTRY.timer(getMetricName(name));
  }

  /**
   * Registers a gauge if it has not been registered.
   *
   * @param name the gauge name
   * @param metric the gauge
   * @param <T> the type
   */
  public static synchronized <T> void registerGaugeIfAbsent(String name, Gauge<T> metric) {
    if (!METRIC_REGISTRY.getGauges().containsKey(name)) {
      METRIC_REGISTRY.register(name, metric);
    }
  }

  /**
   * Resets all the counters to 0 for testing.
   */
  public static void resetAllCounters() {
    for (Map.Entry<String, Counter> entry : METRIC_REGISTRY.getCounters().entrySet()) {
      entry.getValue().dec(entry.getValue().getCount());
    }
  }

  /**
   * @return all the master's metrics in the format of {@link Metric}
   */
  public static List<Metric> allMasterMetrics() {
    return allMetrics(InstanceType.MASTER);
  }

  /**
   * @return all the worker's metrics in the format of {@link Metric}
   */
  public static List<Metric> allWorkerMetrics() {
    return allMetrics(InstanceType.WORKER);
  }

  /**
   * @return all the client's metrics in the format of {@link Metric}
   */
  public static List<Metric> allClientMetrics() {
    return allMetrics(InstanceType.CLIENT);
  }

  /**
   * @return the app ID for this MetricsSystem
   */
  public static synchronized String getAppId() {
    if (mAppId == null) {
      mAppId = Configuration.containsKey(PropertyKey.USER_APP_ID)
          ? Configuration.get(PropertyKey.USER_APP_ID) : IdUtils.createFileSystemContextId();
      LOG.info("Created metrics system with id {}. This ID will be used for identifying metrics "
              + "data from the client. It can be set manually through the {} property",
          mAppId, PropertyKey.Name.USER_APP_ID);
    }
    return mAppId;
  }

  private static List<Metric> allMetrics(MetricsSystem.InstanceType instanceType) {
    List<Metric> metrics = new ArrayList<>();
    for (Entry<String, Gauge> entry : METRIC_REGISTRY.getGauges().entrySet()) {
      if (entry.getKey().startsWith(instanceType.toString())) {
        Object value = entry.getValue().getValue();
        if (!(value instanceof Number)) {
          LOG.warn(
              "The value of metric {} of type {} is not sent to metrics master,"
                  + " only metrics value of number can be collected",
              entry.getKey(), entry.getValue().getClass().getSimpleName());
          continue;
        }
        metrics.add(Metric.from(entry.getKey(), ((Number) value).longValue()));
      }
    }
    for (Entry<String, Counter> entry : METRIC_REGISTRY.getCounters().entrySet()) {
      metrics.add(Metric.from(entry.getKey(), entry.getValue().getCount()));
    }
    for (Entry<String, Meter> entry : METRIC_REGISTRY.getMeters().entrySet()) {
      // TODO(yupeng): From Meter's implementation, getOneMinuteRate can only report at rate of at
      // least seconds. if the client's duration is too short (i.e. < 1s), then getOneMinuteRate
      // would return 0
      metrics.add(Metric.from(entry.getKey(), entry.getValue().getOneMinuteRate()));
    }
    for (Entry<String, Timer> entry : METRIC_REGISTRY.getTimers().entrySet()) {
      metrics.add(Metric.from(entry.getKey(), entry.getValue().getCount()));
    }
    return metrics;
  }

  /**
   * Resets the metric registry and removes all the metrics.
   */
  public static void clearAllMetrics() {
    for (String name : METRIC_REGISTRY.getNames()) {
      METRIC_REGISTRY.remove(name);
    }
  }

  /**
   * Disallows any explicit initialization.
   */
  private MetricsSystem() {
  }
}
