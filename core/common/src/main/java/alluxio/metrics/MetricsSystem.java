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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.MetricType;
import alluxio.grpc.MetricValue;
import alluxio.metrics.sink.Sink;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.JvmAttributeGaugeSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
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
  private static int sResolveTimeout =
      (int) new InstancedConfiguration(ConfigurationUtils.defaults())
          .getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);
  // A map from AlluxioURI to corresponding cached escaped path.
  private static final ConcurrentHashMap<AlluxioURI, String> CACHED_ESCAPED_PATH
      = new ConcurrentHashMap<>();
  // A map from the metric name to its previous reported value
  // This map is used for calculated the counter diff value that will be reported
  private static final Map<String, Long> LAST_REPORTED_METRICS = new HashMap<>();
  // A map that records all the metrics that should be reported and aggregated at leading master
  // from full metric name to its metric type
  private static final Map<String, MetricType> SHOULD_REPORT_METRICS = new ConcurrentHashMap<>();
  // A pattern to get the <instance_type>.<metric_name> from the full metric name
  private static final Pattern METRIC_NAME_PATTERN = Pattern.compile("^(.*?[.].*?)[.].*");
  // A flag telling whether metrics have been reported yet.
  // Using this prevents us from initializing {@link #SHOULD_REPORT_METRICS} more than once
  private static boolean sReported = false;
  // The source of the metrics in this metrics system.
  // It can be set through property keys based on process types.
  // Local hostname will be used if no related property key founds.
  private static Supplier<String> sSourceNameSupplier =
      CommonUtils.memoize(() -> constructSourceName());

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
    PROXY("Proxy"),
    FUSE("Fuse");

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
  public static final String CLUSTER = "Cluster";

  public static final MetricRegistry METRIC_REGISTRY;

  static {
    METRIC_REGISTRY = new MetricRegistry();
    METRIC_REGISTRY.registerAll(new JvmAttributeGaugeSet());
    METRIC_REGISTRY.registerAll(new GarbageCollectorMetricSet());
    METRIC_REGISTRY.registerAll(new MemoryUsageGaugeSet());
    METRIC_REGISTRY.registerAll(new ClassLoadingGaugeSet());
    METRIC_REGISTRY.registerAll(new CachedThreadStatesGaugeSet(5, TimeUnit.SECONDS));
    METRIC_REGISTRY.registerAll(new LogStateCounterSet());
    METRIC_REGISTRY.registerAll(new OperationSystemGaugeSet());
  }

  @GuardedBy("MetricsSystem")
  private static List<Sink> sSinks;

  public static final String SINK_REGEX = "^sink\\.(.+)\\.(.+)";
  private static final TimeUnit MINIMAL_POLL_UNIT = TimeUnit.SECONDS;
  private static final int MINIMAL_POLL_PERIOD = 1;

  /**
   * Starts sinks specified in the configuration.
   * This is an no-op if the sinks have already been started.
   * Note: This has to be called after Alluxio configuration is initialized.
   *
   * @param metricsConfFile the location of the metrics configuration file
   */
  public static void startSinks(String metricsConfFile) {
    synchronized (MetricsSystem.class) {
      if (sSinks != null) {
        LOG.debug("Sinks have already been started.");
        return;
      }
    }
    if (metricsConfFile.isEmpty()) {
      LOG.info("Metrics is not enabled.");
      return;
    }
    MetricsConfig config = new MetricsConfig(metricsConfFile);
    startSinksFromConfig(config);
  }

  /**
   * Constructs the source name of metrics in this {@link MetricsSystem}.
   */
  private static String constructSourceName() {
    PropertyKey sourceKey = null;
    switch (CommonUtils.PROCESS_TYPE.get()) {
      case MASTER:
        sourceKey = PropertyKey.MASTER_HOSTNAME;
        break;
      case WORKER:
        sourceKey = PropertyKey.WORKER_HOSTNAME;
        break;
      case CLIENT:
        sourceKey = PropertyKey.USER_APP_ID;
        break;
      case JOB_MASTER:
        sourceKey = PropertyKey.JOB_MASTER_HOSTNAME;
        break;
      case JOB_WORKER:
        sourceKey = PropertyKey.JOB_WORKER_HOSTNAME;
        break;
      default:
        break;
    }
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    if (sourceKey != null && conf.isSet(sourceKey)) {
      return conf.get(sourceKey);
    }
    String hostName;
    // Avoid throwing RuntimeException when hostname
    // is not resolved on metrics reporting
    try {
      hostName = NetworkAddressUtils.getLocalHostMetricName(sResolveTimeout);
    } catch (RuntimeException e) {
      hostName = "unknown";
      LOG.error("Can't find local host name", e);
    }
    return hostName;
  }

  /**
   * Starts sinks from a given metrics configuration. This is made public for unit test.
   *
   * @param config the metrics config
   */
  public static synchronized void startSinksFromConfig(MetricsConfig config) {
    if (sSinks != null) {
      LOG.debug("Sinks have already been started.");
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
   * Get metrics name based on class.
   *
   * @param obj object for the resource pool
   * @return metrics string
   */
  public static String getResourcePoolMetricName(Object obj) {
    return MetricsSystem.getMetricName("ResourcePool." + obj.getClass().getName() + "."
        + Integer.toHexString(System.identityHashCode(obj)));
  }

  /**
   * Converts a simple string to a qualified metric name based on the process type.
   *
   * @param name the name of the metric
   * @return the metric with instance and id tags
   */
  public static String getMetricName(String name) {
    if (name.startsWith(CLUSTER)) {
      return name;
    }
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
  public static String getMasterMetricName(String name) {
    String result = CACHED_METRICS.get(name);
    if (result != null) {
      return result;
    }
    if (name.startsWith(InstanceType.MASTER.toString())) {
      return CACHED_METRICS.computeIfAbsent(name, n -> name);
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

    // Added for integration tests where process type is always client
    if (name.startsWith(InstanceType.MASTER.toString())
        || name.startsWith(InstanceType.CLUSTER.toString())) {
      return CACHED_METRICS.computeIfAbsent(name, n -> name);
    }
    if (name.startsWith(InstanceType.WORKER.toString())) {
      return getWorkerMetricName(name);
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
   * Builds metric registry names for the job master instance. The pattern is instance.metricName.
   *
   * @param name the metric name
   * @return the metric registry name
   */
  private static String getJobMasterMetricName(String name) {
    if (name.startsWith(InstanceType.JOB_MASTER.toString())) {
      return name;
    }
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
   * instance.metricName.hostname
   *
   * @param instance the instance name
   * @param name the metric name
   * @return the metric registry name
   */
  private static String getMetricNameWithUniqueId(InstanceType instance, String name) {
    if (name.startsWith(instance.toString())) {
      return Joiner.on(".").join(name, sSourceNameSupplier.get());
    }
    return Joiner.on(".").join(instance, name, sSourceNameSupplier.get());
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
    int len = pieces.length;
    if (len <= 1) {
      return metricsName;
    }
    // Master metrics doesn't have hostname included.
    if (!pieces[0].equals(MetricsSystem.InstanceType.MASTER.toString())
        && !pieces[0].equals(InstanceType.CLUSTER.toString())
        && pieces.length > 2) {
      pieces[len - 1] = null;
    }
    pieces[0] = null;
    return Joiner.on(".").skipNulls().join(pieces);
  }

  /**
   * Escapes a URI, replacing "/" with "%2F".
   * Replaces "." with "%2E" because dots are used as tag separators in metric names.
   * Replaces "%" with "%25" because it now has special use.
   * So when the URI is used in a metric name, the "." and "/" won't be interpreted as
   * path separators unescaped and interfere with the internal logic of AlluxioURI.
   *
   * @param uri the URI to escape
   * @return the string representing the escaped URI
   */
  public static String escape(AlluxioURI uri) {
    return CACHED_ESCAPED_PATH.computeIfAbsent(uri,
        u -> u.toString().replace("%", "%25")
            .replace("/", "%2F").replace(".", "%2E"));
  }

  /**
   * Unescapes a URI, reverts it to before the escape, to display it correctly.
   *
   * @param uri the escaped URI to unescape
   * @return the string representing the unescaped original URI
   */
  public static String unescape(String uri) {
    return uri.replace("%2F", "/").replace("%2E", ".").replace("%25", "%");
  }

  // Some helper functions.

  /**
   * Get or add counter with the given name.
   * The counter stores in the metrics system is never removed but may reset to zero.
   *
   * @param name the name of the metric
   * @return a counter object with the qualified metric name
   */
  public static Counter counter(String name) {
    return METRIC_REGISTRY.counter(getMetricName(name));
  }

  /**
   * Get or add counter with the given name with tags.
   * The counter stores in the metrics system is never removed but may reset to zero.
   * If this metric can be aggregated at cluster level and should report to leading master,
   * add it to the should report metrics map.
   *
   * This method is added to add worker metrics with ufs tags into the should report metrics map.
   *
   * @param name the metric name
   * @param shouldReport whether this metric should be reported
   * @param tags the tag name and tag value pairs
   * @return a counter object with the qualified metric name
   */
  public static Counter counterWithTags(String name, boolean shouldReport, String... tags) {
    String fullName = getMetricName(Metric.getMetricNameWithTags(name, tags));
    if (shouldReport) {
      SHOULD_REPORT_METRICS.putIfAbsent(fullName, MetricType.COUNTER);
    }
    return METRIC_REGISTRY.counter(fullName);
  }

  /**
   * Get or add meter with the given name.
   * Please don't save the Meter instance since
   * the returned Meter instance may not be used due to {@link #resetAllMetrics}
   *
   * @param name the name of the metric
   * @return a meter object with the qualified metric name
   */
  public static Meter meter(String name) {
    return METRIC_REGISTRY.meter(getMetricName(name));
  }

  /**
   * Get or add meter with the given name.
   * The returned meter may be changed due to {@link #resetAllMetrics}
   * If this metric can be aggregated at cluster level and should report to leading master,
   * add it to the should report metrics map.
   *
   * This method is added to add worker metrics with ufs tags into the should report metrics map.
   *
   * @param name the name of the metric
   * @param shouldReport whether this metric should be reported
   * @param tags the tag name and tag value pairs
   * @return a meter object with the qualified metric name
   */
  public static Meter meterWithTags(String name, boolean shouldReport, String... tags) {
    String fullName = getMetricName(Metric.getMetricNameWithTags(name, tags));
    if (shouldReport) {
      SHOULD_REPORT_METRICS.putIfAbsent(fullName, MetricType.METER);
    }
    return METRIC_REGISTRY.meter(fullName);
  }

  /**
   * Get or add timer with the given name.
   * Please don't save the Timer instance since
   * the returned Timer instance may not be used due to {@link #resetAllMetrics}
   *
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
    if (!METRIC_REGISTRY.getMetrics().containsKey(name)) {
      METRIC_REGISTRY.register(name, metric);
    }
  }

  /**
   * Registers a cached gauge if it has not been registered.
   *
   * @param name the gauge name
   * @param metric the gauge
   * @param <T> the type
   */
  public static synchronized <T> void registerCachedGaugeIfAbsent(String name, Gauge<T> metric) {
    if (!METRIC_REGISTRY.getMetrics().containsKey(name)) {
      METRIC_REGISTRY.register(name, new CachedGauge<T>(10, TimeUnit.MINUTES) {
        @Override
        protected T loadValue() {
          return metric.getValue();
        }
      });
    }
  }

  /**
   * This method is used to return a list of RPC metric objects which will be sent to the
   * MetricsMaster.
   *
   * It is mainly useful for metrics which have a {@link MetricType} of COUNTER. The metric values
   * with this type will be only include value of the difference since the last time that
   * {@code reportMetrics} was called.
   *
   * By sending only the difference since the last RPC, this method will allow the master to
   * track the metrics for a given worker, even if the worker is restarted.
   *
   * The synchronized keyword is added for correctness with {@link #resetAllMetrics}
   */
  private static synchronized List<alluxio.grpc.Metric> reportMetrics(InstanceType instanceType) {
    if (!sReported) {
      initShouldReportMetrics(instanceType);
      sReported = true;
    }
    List<alluxio.grpc.Metric> rpcMetrics = new ArrayList<>(20);
    // Use the getMetrics() call instead of getGauges(),getCounters()... to avoid
    // unneeded metrics copy
    Map<String, com.codahale.metrics.Metric> metrics = METRIC_REGISTRY.getMetrics();

    for (Map.Entry<String, MetricType> entry : SHOULD_REPORT_METRICS.entrySet()) {
      com.codahale.metrics.Metric metric = metrics.get(entry.getKey());
      if (metric == null) {
        // This metric does not registered in the metric registry
        continue;
      }
      // Currently all metrics that should be reported are all counters,
      // the logic here is to support reporting metrics of all types for future convenience
      if (metric instanceof Counter) {
        Counter counter = (Counter) metric;
        long value = counter.getCount();
        Long prev = LAST_REPORTED_METRICS.replace(entry.getKey(), value);
        // On restarts counters will be reset to 0, so whatever the value is the first time
        // this method is called represents the value which should be added to the master's
        // counter.
        if (prev == null) {
          LAST_REPORTED_METRICS.put(entry.getKey(), value);
        }
        double diff = prev != null ? value - prev : value;
        if (diff != 0) { // Only report non-zero counter values
          rpcMetrics.add(Metric.from(entry.getKey(), diff, MetricType.COUNTER).toProto());
        }
      } else if (metric instanceof Gauge) {
        Gauge gauge = (Gauge) metric;
        if (!(gauge.getValue() instanceof Number)) {
          LOG.debug("The value of metric {} of type {} is not sent to metrics master,"
                  + " only metrics value of number can be collected",
              entry.getKey(), gauge.getValue().getClass().getSimpleName());
          continue;
        }
        rpcMetrics.add(Metric.from(entry.getKey(),
            ((Number) gauge.getValue()).longValue(), MetricType.GAUGE).toProto());
      } else if (metric instanceof Meter) {
        Meter meter = (Meter) metric;
        // Note that one minute rate may return 0 in the first several seconds
        // that a value marked. For clients, especially short-life clients,
        // the minute rates will be zero for their whole life.
        // That's why all throughput meters are not aggregated at cluster level.
        rpcMetrics.add(Metric.from(entry.getKey(), meter.getOneMinuteRate(),
            MetricType.METER).toProto());
      } else if (metric instanceof Timer) {
        Timer timer = (Timer) metric;
        rpcMetrics.add(Metric.from(entry.getKey(), timer.getCount(),
            MetricType.TIMER).toProto());
      } else {
        LOG.warn("Metric {} has invalid metric type {} which cannot be reported",
            entry.getKey(), entry.getValue());
      }
    }
    return rpcMetrics;
  }

  /**
   * @return the worker metrics to send via RPC
   */
  public static List<alluxio.grpc.Metric> reportWorkerMetrics() {
    long start = System.currentTimeMillis();
    List<alluxio.grpc.Metric> metricsList = reportMetrics(InstanceType.WORKER);
    LOG.debug("Get the worker metrics list to report to leading master in {}ms",
        System.currentTimeMillis() - start);
    return metricsList;
  }

  /**
   * @return the client metrics to send via RPC
   */
  public static List<alluxio.grpc.Metric> reportClientMetrics() {
    long start = System.currentTimeMillis();
    List<alluxio.grpc.Metric> metricsList = reportMetrics(InstanceType.CLIENT);
    LOG.debug("Get the client metrics list to report to leading master in {}ms",
        System.currentTimeMillis() - start);
    return metricsList;
  }

  /**
   * Gets all the master metrics belongs to the given metric names.
   *
   * @param metricNames the name of the metrics to get
   * @return a metric map from metric name to metrics with this name
   */
  public static Map<String, Set<Metric>> getMasterMetrics(Set<String> metricNames) {
    Map<String, Set<Metric>> res = new HashMap<>();
    for (Map.Entry<String, com.codahale.metrics.Metric> entry
        : METRIC_REGISTRY.getMetrics().entrySet()) {
      Matcher matcher = METRIC_NAME_PATTERN.matcher(entry.getKey());
      if (matcher.matches()) {
        String name = matcher.group(1);
        if (metricNames.contains(name)) {
          res.computeIfAbsent(name, m -> new HashSet<>())
              .add(getAlluxioMetricFromCodahaleMetric(entry.getKey(), entry.getValue()));
        }
      }
    }
    return res;
  }

  /**
   * Gets metric with the given full metric name.
   *
   * @param fullName the full name of the metric to get
   * @return a metric set with the master metric of the given metric name
   */
  @Nullable
  public static Metric getMetricValue(String fullName) {
    Map<String, com.codahale.metrics.Metric> metricMap = METRIC_REGISTRY.getMetrics();
    com.codahale.metrics.Metric metric = metricMap.get(fullName);
    if (metric == null) {
      return null;
    }
    return getAlluxioMetricFromCodahaleMetric(fullName, metric);
  }

  @Nullable
  private static Metric getAlluxioMetricFromCodahaleMetric(String name,
      com.codahale.metrics.Metric metric) {
    if (metric instanceof Gauge) {
      Gauge gauge = (Gauge) metric;
      return Metric.from(name, ((Number) gauge.getValue()).longValue(), MetricType.GAUGE);
    } else if (metric instanceof Counter) {
      Counter counter = (Counter) metric;
      return Metric.from(name, counter.getCount(), MetricType.COUNTER);
    } else if (metric instanceof Meter) {
      Meter meter = (Meter) metric;
      return Metric.from(name, meter.getOneMinuteRate(), MetricType.METER);
    } else if (metric instanceof Timer) {
      Timer timer = (Timer) metric;
      return Metric.from(name, timer.getCount(), MetricType.TIMER);
    }
    LOG.warn("Metric {} has invalid metric type {}", name, metric.getClass().getName());
    return null;
  }

  /**
   * @return a map of all metrics stored in the current node
   *         from metric name to {@link MetricValue}
   */
  public static Map<String, MetricValue> allMetrics() {
    Map<String, MetricValue> metricsMap = new HashMap<>();
    for (Map.Entry<String, com.codahale.metrics.Metric> entry
        : METRIC_REGISTRY.getMetrics().entrySet()) {
      MetricValue.Builder valueBuilder = MetricValue.newBuilder();
      com.codahale.metrics.Metric metric = entry.getValue();
      if (metric instanceof Gauge) {
        Object value = ((Gauge) metric).getValue();
        if (value instanceof Number) {
          valueBuilder.setDoubleValue(((Number) value).doubleValue());
        } else {
          valueBuilder.setStringValue(value.toString());
        }
        valueBuilder.setMetricType(MetricType.GAUGE);
      } else if (metric instanceof Counter) {
        valueBuilder.setMetricType(MetricType.COUNTER)
            .setDoubleValue(((Counter) metric).getCount());
      } else if (metric instanceof Meter) {
        valueBuilder.setMetricType(MetricType.METER)
            .setDoubleValue(((Meter) metric).getOneMinuteRate());
      } else if (metric instanceof Timer) {
        valueBuilder.setMetricType(MetricType.TIMER)
            .setDoubleValue(((Timer) metric).getCount());
      } else {
        LOG.warn("Metric {} has invalid metric type {}",
            entry.getKey(), metric.getClass().getName());
        continue;
      }
      metricsMap.put(entry.getKey(), valueBuilder.build());
    }
    return metricsMap;
  }

  /**
   * Initialize the {@link #SHOULD_REPORT_METRICS}. This should be called only once.
   *
   * Note that this method is able to catch most of the should report metrics
   * except worker metrics with ufs tags.
   *
   * @param instanceType the instance type
   */
  @VisibleForTesting
  public static void initShouldReportMetrics(InstanceType instanceType) {
    Set<MetricKey> metricKeys = MetricKey.allShouldReportMetricKeys(instanceType);
    for (MetricKey metricKey : metricKeys) {
      SHOULD_REPORT_METRICS.putIfAbsent(
          getMetricNameWithUniqueId(instanceType, metricKey.getName()),
          metricKey.getMetricType());
    }
  }

  /**
   * Resets all the metrics in the MetricsSystem.
   *
   * This method is not thread-safe and should be used sparingly.
   */
  public static synchronized void resetAllMetrics() {
    long startTime = System.currentTimeMillis();
    // Gauge metrics don't need to be changed because they calculate value when getting them
    // Counters can be reset to zero values.
    for (Counter counter : METRIC_REGISTRY.getCounters().values()) {
      counter.dec(counter.getCount());
    }

    // No reset logic exist in Meter, a remove and add combination is needed
    for (String meterName : METRIC_REGISTRY.getMeters().keySet()) {
      METRIC_REGISTRY.remove(meterName);
      METRIC_REGISTRY.meter(meterName);
    }

    // No reset logic exist in Timer, a remove and add combination is needed
    for (String timerName : METRIC_REGISTRY.getTimers().keySet()) {
      METRIC_REGISTRY.remove(timerName);
      METRIC_REGISTRY.timer(timerName);
    }
    LAST_REPORTED_METRICS.clear();
    LOG.info("Reset all metrics in the metrics system in {}ms",
        System.currentTimeMillis() - startTime);
  }

  /**
   * Resets the metric registry and removes all the metrics.
   */
  @VisibleForTesting
  public static void clearAllMetrics() {
    for (String name : METRIC_REGISTRY.getNames()) {
      METRIC_REGISTRY.remove(name);
    }
  }

  /**
   * Resets all counters to 0 and unregisters gauges for testing.
   */
  @VisibleForTesting
  public static void resetCountersAndGauges() {
    for (Map.Entry<String, Counter> entry : METRIC_REGISTRY.getCounters().entrySet()) {
      entry.getValue().dec(entry.getValue().getCount());
    }
    for (String gauge : METRIC_REGISTRY.getGauges().keySet()) {
      METRIC_REGISTRY.remove(gauge);
    }
  }

  /**
   * A timer context with multiple timers.
   */
  public static class MultiTimerContext implements AutoCloseable {
    private final Timer[] mTimers;
    private final long mStartTime;

    /**
     * @param timers timers associated with this context
     */
    public MultiTimerContext(Timer... timers) {
      mTimers = timers;
      mStartTime = System.nanoTime();
    }

    /**
     * Updates the timer with the difference between current and start time. Call to this method
     * will not reset the start time. Multiple calls result in multiple updates.
     */
    @Override
    public void close() {
      final long elapsed = System.nanoTime() - mStartTime;
      for (Timer timer : mTimers) {
        timer.update(elapsed, TimeUnit.NANOSECONDS);
      }
    }
  }

  /**
   * Disallows any explicit initialization.
   */
  private MetricsSystem() {
  }
}
