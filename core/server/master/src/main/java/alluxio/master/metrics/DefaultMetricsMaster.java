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

package alluxio.master.metrics;

import alluxio.Constants;
import alluxio.clock.SystemClock;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.GrpcService;
import alluxio.grpc.MetricValue;
import alluxio.grpc.ServiceType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.CoreMaster;
import alluxio.master.CoreMasterContext;
import alluxio.master.journal.NoopJournaled;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.MultiValueMetricsAggregator;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.aggregator.SingleTagValueAggregator;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Default implementation of the metrics master.
 */
public class DefaultMetricsMaster extends CoreMaster implements MetricsMaster, NoopJournaled {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsMaster.class);
  // A map from the to be aggregated metric name to aggregator itself
  // This registry only holds aggregator for master metrics
  private final Map<String, MultiValueMetricsAggregator> mAggregatorRegistry = new HashMap<>();
  private final MetricsStore mMetricsStore;

  /**
   * Creates a new instance of {@link MetricsMaster}.
   *
   * @param masterContext the context for metrics master
   */
  DefaultMetricsMaster(CoreMasterContext masterContext) {
    this(masterContext, new SystemClock(),
        ExecutorServiceFactories.fixedThreadPool(Constants.METRICS_MASTER_NAME,
            ServerConfiguration.getInt(PropertyKey.MASTER_METRICS_SERVICE_THREADS)));
  }

  /**
   * Creates a new instance of {@link MetricsMaster}.
   *
   * @param masterContext the context for metrics master
   * @param clock the clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  DefaultMetricsMaster(CoreMasterContext masterContext, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    super(masterContext, clock, executorServiceFactory);
    mMetricsStore = new MetricsStore(mClock);
    registerAggregators();
  }

  @VisibleForTesting
  protected void addAggregator(MultiValueMetricsAggregator aggregator) {
    mAggregatorRegistry.put(aggregator.getFilterMetricName(), aggregator);
  }

  private void updateMultiValueMasterMetrics() {
    Map<String, Set<Metric>> masterMetricsMap
        = MetricsSystem.getMasterMetrics(mAggregatorRegistry.keySet());
    for (Map.Entry<String, Set<Metric>> entry : masterMetricsMap.entrySet()) {
      MultiValueMetricsAggregator aggregator = mAggregatorRegistry.get(entry.getKey());
      for (Entry<String, Long> updated : aggregator.updateValues(entry.getValue()).entrySet()) {
        MetricsSystem.registerGaugeIfAbsent(updated.getKey(), new Gauge<Object>() {
          @Override
          public Object getValue() {
            return aggregator.getValue(updated.getKey());
          }
        });
      }
    }
  }

  private void registerAggregators() {
    // worker metrics
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_READ_DIRECT.getName(),
        MetricKey.CLUSTER_BYTES_READ_DIRECT_THROUGHPUT.getName());
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_READ_REMOTE.getName(),
        MetricKey.CLUSTER_BYTES_READ_REMOTE_THROUGHPUT.getName());
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_READ_DOMAIN.getName(),
        MetricKey.CLUSTER_BYTES_READ_DOMAIN_THROUGHPUT.getName());
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_WRITTEN_REMOTE.getName(),
        MetricKey.CLUSTER_BYTES_WRITTEN_REMOTE_THROUGHPUT.getName());
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN.getName(),
        MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN_THROUGHPUT.getName());
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName(),
        MetricKey.CLUSTER_BYTES_READ_UFS_THROUGHPUT.getName());
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_ALL.getName(),
        MetricKey.CLUSTER_BYTES_WRITTEN_UFS_THROUGHPUT.getName());
    // client metrics
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_READ_LOCAL.getName(),
        MetricKey.CLUSTER_BYTES_READ_LOCAL_THROUGHPUT.getName());
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_WRITTEN_LOCAL.getName(),
        MetricKey.CLUSTER_BYTES_WRITTEN_LOCAL_THROUGHPUT.getName());

    // TODO(lu) Create a template for dynamically construct MetricKey
    for (MetricInfo.UfsOps ufsOp : MetricInfo.UfsOps.values()) {
      addAggregator(new SingleTagValueAggregator(MetricInfo.UFS_OP_PREFIX + ufsOp,
          MetricsSystem.getMasterMetricName(ufsOp.toString()), MetricInfo.TAG_UFS));
    }
  }

  /**
   * Registers the corresponding throughput of the given counter.
   *
   * @param counterName the counter to get value of
   * @param throughputName the gauge throughput name to be registered
   */
  @VisibleForTesting
  protected void registerThroughputGauge(String counterName, String throughputName) {
    MetricsSystem.registerGaugeIfAbsent(throughputName,
        new Gauge<Object>() {
          @Override
          public Object getValue() {
            // Divide into two lines so uptime is always zero or positive
            long lastClearTime = mMetricsStore.getLastClearTime();
            long uptime = (mClock.millis() - lastClearTime)
                / Constants.MINUTE_MS;
            long value = MetricsSystem.counter(counterName).getCount();
            // The value is bytes per minute
            return uptime <= 0 ? value : value / uptime;
          }
        });
  }

  @Override
  public String getName() {
    return Constants.METRICS_MASTER_NAME;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
    services.put(ServiceType.METRICS_MASTER_CLIENT_SERVICE,
        new GrpcService(getMasterServiceHandler()));
    return services;
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    if (isLeader) {
      mMetricsStore.initMetricKeys();
      mMetricsStore.clear();
      getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_CLUSTER_METRICS_UPDATER, new ClusterMetricsUpdater(),
          ServerConfiguration.getMs(PropertyKey.MASTER_CLUSTER_METRICS_UPDATE_INTERVAL),
          ServerConfiguration.global(), mMasterContext.getUserState()));
    }
  }

  @Override
  public void clientHeartbeat(String source, List<Metric> metrics) {
    getExecutorService().submit(() -> mMetricsStore.putClientMetrics(source, metrics));
  }

  @Override
  public MetricsMasterClientServiceHandler getMasterServiceHandler() {
    return new MetricsMasterClientServiceHandler(this);
  }

  @Override
  public void workerHeartbeat(String source, List<Metric> metrics) {
    getExecutorService().submit(() -> mMetricsStore.putWorkerMetrics(source, metrics));
  }

  @Override
  public void clearMetrics() {
    mMetricsStore.clear();
  }

  @Override
  public Map<String, MetricValue> getMetrics() {
    return MetricsSystem.allMetrics();
  }

  /**
   * Heartbeat executor that updates the cluster metrics.
   */
  private class ClusterMetricsUpdater implements HeartbeatExecutor {
    @Override
    public void heartbeat() throws InterruptedException {
      updateMultiValueMasterMetrics();
    }

    @Override
    public void close() {
      // nothing to clean up
    }
  }
}
