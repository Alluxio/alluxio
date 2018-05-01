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
import alluxio.master.AbstractMaster;
import alluxio.master.MasterContext;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsAggregator;
import alluxio.metrics.MetricsFilter;
import alluxio.metrics.MetricsStore;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;
import alluxio.metrics.aggregator.SumInstancesAggregator;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of the metrics master.
 */
public class DefaultMetricsMaster extends AbstractMaster implements MetricsMaster {
  private final Map<String, MetricsAggregator> mMetricsAggregatorRegistry = new HashMap<>();
  private final MetricsStore mMetricsStore;

  /**
   * Creates a new instance of {@link MetricsMaster}.
   *
   * @param masterContext the context for metrics master
   */
  protected DefaultMetricsMaster(MasterContext masterContext) {
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
  protected DefaultMetricsMaster(MasterContext masterContext, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    super(masterContext, clock, executorServiceFactory);
    mMetricsStore = masterContext.getMetricsStore();
    registerAggregators();
  }

  @VisibleForTesting
  protected void addAggregator(MetricsAggregator aggregator) {
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

  private void registerAggregators() {
    addAggregator(new SumInstancesAggregator(MetricsSystem.WORKER_INSTANCE,
        WorkerMetrics.BYTES_READ_ALLUXIO));
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
    return services;
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
  }
}
