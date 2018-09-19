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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.clock.SystemClock;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterContext;
import alluxio.metrics.ClientMetrics;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsAggregator;
import alluxio.metrics.MetricsFilter;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.MultiValueMetricsAggregator;
import alluxio.metrics.SingleValueAggregator;
import alluxio.metrics.WorkerMetrics;
import alluxio.metrics.aggregator.SingleTagValueAggregator;
import alluxio.metrics.aggregator.SumInstancesAggregator;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.thrift.MetricsMasterClientService;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Default implementation of the metrics master.
 */
public class DefaultMetricsMaster extends AbstractMaster implements MetricsMaster {
  private final Map<String, MetricsAggregator> mMetricsAggregatorRegistry = new HashMap<>();
  private final Set<MultiValueMetricsAggregator> mMultiValueMetricsAggregatorRegistry =
      new HashSet<>();
  private final MetricsStore mMetricsStore;
  private final HeartbeatThread mClusterMetricsUpdater;

  /**
   * Creates a new instance of {@link MetricsMaster}.
   *
   * @param masterContext the context for metrics master
   */
  DefaultMetricsMaster(MasterContext masterContext) {
    this(masterContext, new SystemClock(), ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.METRICS_MASTER_NAME, 2));
  }

  /**
   * Creates a new instance of {@link MetricsMaster}.
   *
   * @param masterContext the context for metrics master
   * @param clock the clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  DefaultMetricsMaster(MasterContext masterContext, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    super(masterContext, clock, executorServiceFactory);
    mMetricsStore = new MetricsStore();
    registerAggregators();
    mClusterMetricsUpdater =
        new HeartbeatThread(HeartbeatContext.MASTER_CLUSTER_METRICS_UPDATER,
            new ClusterMetricsUpdater(),
            Configuration.getMs(PropertyKey.MASTER_CLUSTER_METRICS_UPDATE_INTERVAL));
  }

  @VisibleForTesting
  protected void addAggregator(SingleValueAggregator aggregator) {
    mMetricsAggregatorRegistry.put(aggregator.getName(), aggregator);
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getClusterMetricName(aggregator.getName()),
        new Gauge<Object>() {
          @Override
          public Object getValue() {
            Map<MetricsFilter, Set<Metric>> metrics = new HashMap<>();
            for (MetricsFilter filter : aggregator.getFilters()) {
              metrics.put(filter, mMetricsStore
                  .getMetricsByInstanceTypeAndName(filter.getInstanceType(), filter.getName()));
            }
            return aggregator.getValue(metrics);
          }
        });
  }

  @VisibleForTesting
  protected void addAggregator(MultiValueMetricsAggregator aggregator) {
    mMultiValueMetricsAggregatorRegistry.add(aggregator);
  }

  private void updateMultiValueMetrics() {
    for (MultiValueMetricsAggregator aggregator : mMultiValueMetricsAggregatorRegistry) {
      Map<MetricsFilter, Set<Metric>> metrics = new HashMap<>();
      for (MetricsFilter filter : aggregator.getFilters()) {
        metrics.put(filter, mMetricsStore.getMetricsByInstanceTypeAndName(filter.getInstanceType(),
            filter.getName()));
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
    addAggregator(new SumInstancesAggregator(WorkerMetrics.BYTES_READ_ALLUXIO,
        MetricsSystem.InstanceType.WORKER, WorkerMetrics.BYTES_READ_ALLUXIO));
    addAggregator(new SumInstancesAggregator(WorkerMetrics.BYTES_READ_ALLUXIO_THROUGHPUT,
        MetricsSystem.InstanceType.WORKER, WorkerMetrics.BYTES_READ_ALLUXIO_THROUGHPUT));
    addAggregator(new SumInstancesAggregator(WorkerMetrics.BYTES_READ_UFS_ALL,
        MetricsSystem.InstanceType.WORKER, WorkerMetrics.BYTES_READ_UFS));
    addAggregator(new SumInstancesAggregator(WorkerMetrics.BYTES_READ_UFS_THROUGHPUT,
        MetricsSystem.InstanceType.WORKER, WorkerMetrics.BYTES_READ_UFS_THROUGHPUT));

    addAggregator(new SumInstancesAggregator(WorkerMetrics.BYTES_WRITTEN_ALLUXIO,
        MetricsSystem.InstanceType.WORKER, WorkerMetrics.BYTES_WRITTEN_ALLUXIO));
    addAggregator(new SumInstancesAggregator(WorkerMetrics.BYTES_WRITTEN_ALLUXIO_THROUGHPUT,
        MetricsSystem.InstanceType.WORKER, WorkerMetrics.BYTES_WRITTEN_ALLUXIO_THROUGHPUT));
    addAggregator(new SumInstancesAggregator(WorkerMetrics.BYTES_WRITTEN_UFS_ALL,
        MetricsSystem.InstanceType.WORKER, WorkerMetrics.BYTES_WRITTEN_UFS));
    addAggregator(new SumInstancesAggregator(WorkerMetrics.BYTES_WRITTEN_UFS_THROUGHPUT,
        MetricsSystem.InstanceType.WORKER, WorkerMetrics.BYTES_WRITTEN_UFS_THROUGHPUT));

    // client metrics
    addAggregator(new SumInstancesAggregator(ClientMetrics.BYTES_READ_LOCAL,
        MetricsSystem.InstanceType.CLIENT, ClientMetrics.BYTES_READ_LOCAL));
    addAggregator(new SumInstancesAggregator(ClientMetrics.BYTES_READ_LOCAL_THROUGHPUT,
        MetricsSystem.InstanceType.CLIENT, ClientMetrics.BYTES_READ_LOCAL_THROUGHPUT));

    // multi-value aggregators
    addAggregator(new SingleTagValueAggregator(WorkerMetrics.BYTES_READ_UFS,
        MetricsSystem.InstanceType.WORKER, WorkerMetrics.BYTES_READ_UFS, WorkerMetrics.TAG_UFS));
    addAggregator(new SingleTagValueAggregator(WorkerMetrics.BYTES_WRITTEN_UFS,
        MetricsSystem.InstanceType.WORKER, WorkerMetrics.BYTES_WRITTEN_UFS, WorkerMetrics.TAG_UFS));
    for (WorkerMetrics.UfsOps ufsOp : WorkerMetrics.UfsOps.values()) {
      addAggregator(new SingleTagValueAggregator(WorkerMetrics.UFS_OP_PREFIX + ufsOp,
          MetricsSystem.InstanceType.MASTER, ufsOp.toString(),
          WorkerMetrics.TAG_UFS));
    }
  }

  @Override
  public String getName() {
    return Constants.METRICS_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // Do nothing, for now the metrics master is state-less
  }

  @Override
  public void resetState() {
    mMetricsStore.clear();
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return Collections.emptyIterator();
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.METRICS_MASTER_CLIENT_SERVICE_NAME,
        new MetricsMasterClientService.Processor<>(getMasterServiceHandler()));
    return services;
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    if (isLeader) {
      getExecutorService().submit(mClusterMetricsUpdater);
    }
  }

  @Override
  public void clientHeartbeat(String clientId, String hostname, List<Metric> metrics) {
    mMetricsStore.putClientMetrics(hostname, clientId, metrics);
  }

  @Override
  public MetricsMasterClientServiceHandler getMasterServiceHandler() {
    return new MetricsMasterClientServiceHandler(this);
  }

  @Override
  public void workerHeartbeat(String hostname, List<Metric> metrics) {
    mMetricsStore.putWorkerMetrics(hostname, metrics);
  }

  /**
   * Heartbeat executor that updates the cluster metrics.
   */
  private class ClusterMetricsUpdater implements HeartbeatExecutor {
    @Override
    public void heartbeat() throws InterruptedException {
      updateMultiValueMetrics();
    }

    @Override
    public void close() {
      // nothing to clean up
    }
  }
}
