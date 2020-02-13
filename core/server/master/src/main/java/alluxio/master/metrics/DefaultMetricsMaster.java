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
import alluxio.metrics.MetricsFilter;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Default implementation of the metrics master.
 */
public class DefaultMetricsMaster extends CoreMaster implements MetricsMaster, NoopJournaled {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsMaster.class);
  private final Set<MultiValueMetricsAggregator> mMultiValueMetricsAggregatorRegistry =
      new HashSet<>();
  private final MetricsStore mMetricsStore;

  /**
   * Creates a new instance of {@link MetricsMaster}.
   *
   * @param masterContext the context for metrics master
   */
  DefaultMetricsMaster(CoreMasterContext masterContext) {
    this(masterContext, new SystemClock(),
        ExecutorServiceFactories.cachedThreadPool(Constants.METRICS_MASTER_NAME));
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
    mMetricsStore = new MetricsStore();
    registerAggregators();
  }

  @VisibleForTesting
  protected void addAggregator(MultiValueMetricsAggregator aggregator) {
    mMultiValueMetricsAggregatorRegistry.add(aggregator);
  }

  private void updateMultiValueMasterMetrics() {
    for (MultiValueMetricsAggregator aggregator : mMultiValueMetricsAggregatorRegistry) {
      Map<MetricsFilter, Set<Metric>> metrics = new HashMap<>();
      for (MetricsFilter filter : aggregator.getFilters()) {
        metrics.put(filter, MetricsSystem.getMasterMetric(filter.getName()));
      }
      for (Entry<String, Long> entry : aggregator.updateValues(metrics).entrySet()) {
        MetricsSystem.registerGaugeIfAbsent(entry.getKey(), new Gauge<Object>() {
          @Override
          public Object getValue() {
            return aggregator.getValue(entry.getKey());
          }
        });
      }
    }
  }

  private void registerAggregators() {
    // worker metrics
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_READ_ALLUXIO.getName(),
        MetricKey.CLUSTER_BYTES_READ_ALLUXIO_THROUGHPUT.getName());
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_READ_DOMAIN.getName(),
        MetricKey.CLUSTER_BYTES_READ_DOMAIN_THROUGHPUT.getName());
    registerThroughputGauge(MetricKey.CLUSTER_BYTES_WRITTEN_ALLUXIO.getName(),
        MetricKey.CLUSTER_BYTES_WRITTEN_ALLUXIO_THROUGHPUT.getName());
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
          MetricsSystem.InstanceType.MASTER, ufsOp.toString(),
          MetricInfo.TAG_UFS));
    }
  }

  /**
   * Registers the corresponding throughput of the given counter.
   *
   * @param counterName the counter to get value of
   * @param throughputName the gauge throughput name to be registered
   */
  private void registerThroughputGauge(String counterName, String throughputName) {
    MetricsSystem.registerGaugeIfAbsent(throughputName,
        new Gauge<Object>() {
          @Override
          public Object getValue() {
            long uptime = (System.currentTimeMillis() - mMetricsStore.getLastClearTime())
                / Constants.SECOND_MS;
            return uptime == 0 ? 0 : MetricsSystem.counter(counterName).getCount() / uptime;
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
      mMetricsStore.clear();
      mMetricsStore.init();
      getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_CLUSTER_METRICS_UPDATER, new ClusterMetricsUpdater(),
          ServerConfiguration.getMs(PropertyKey.MASTER_CLUSTER_METRICS_UPDATE_INTERVAL),
          ServerConfiguration.global(), mMasterContext.getUserState()));
    }
  }

  @Override
  public void clientHeartbeat(String source, List<Metric> metrics) {
    mMetricsStore.putClientMetrics(source, metrics);
  }

  @Override
  public MetricsMasterClientServiceHandler getMasterServiceHandler() {
    return new MetricsMasterClientServiceHandler(this);
  }

  @Override
  public void workerHeartbeat(String source, List<Metric> metrics) {
    mMetricsStore.putWorkerMetrics(source, metrics);
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
