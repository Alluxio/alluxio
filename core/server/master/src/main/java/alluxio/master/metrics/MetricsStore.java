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

import alluxio.grpc.MetricType;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.MetricsSystem.InstanceType;
import alluxio.resource.LockResource;

import com.codahale.metrics.Counter;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A store of metrics containing the metrics collected from workers and clients.
 */
@ThreadSafe
public class MetricsStore {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsStore.class);
  // The following fields are added for reducing the string processing
  // for MetricKey.WORKER_BYTES_READ_UFS and MetricKey.WORKER_BYTES_WRITTEN_UFS
  private static final String BYTES_READ_UFS = "BytesReadPerUfs";
  private static final String BYTES_WRITTEN_UFS = "BytesWrittenPerUfs";

  private final Clock mClock;

  // The lock to guarantee that only one thread is clearing the metrics
  // and no other interaction with worker/client metrics set is allowed during metrics clearing
  private final ReentrantReadWriteLock mLock;

  // The time of the most recent metrics store clearance.
  // This tracks when the cluster counters start aggregating from the reported metrics.
  @GuardedBy("mLock")
  private long mLastClearTime;

  /**
   * A map from the cluster counter key representing the metrics to be aggregated
   * to the corresponding aggregated cluster Counter.
   * For example, Counter of cluster.BytesReadRemote is aggregated from
   * the worker reported worker.BytesReadRemote.
   *
   * Exceptions are the BytesRead/WrittenUfs metrics which records
   * the actual cluster metrics name to its Counter directly.
   */
  @GuardedBy("mLock")
  private final ConcurrentHashMap<ClusterCounterKey, Counter> mClusterCounters;

  /**
   * Constructs a new {@link MetricsStore}.
   *
   * @param clock the clock to get time of
   */
  public MetricsStore(Clock clock) {
    mClock = clock;
    mLastClearTime = clock.millis();
    mLock = new ReentrantReadWriteLock();
    mClusterCounters = new ConcurrentHashMap<>();
  }

  /**
   * Put the metrics from a worker with a source name.
   *
   * @param source the source name
   * @param metrics the new worker metrics
   */
  public void putWorkerMetrics(String source, List<Metric> metrics) {
    if (metrics.isEmpty() || source == null) {
      return;
    }
    try (LockResource r = new LockResource(mLock.readLock())) {
      putReportedMetrics(InstanceType.WORKER, metrics);
    }
    LOG.debug("Put {} metrics of worker {}", metrics.size(), source);
  }

  /**
   * Put the metrics from a client with a source name.
   *
   * @param source the source name
   * @param metrics the new metrics
   */
  public void putClientMetrics(String source, List<Metric> metrics) {
    if (metrics.isEmpty() || source == null) {
      return;
    }
    try (LockResource r = new LockResource(mLock.readLock())) {
      putReportedMetrics(InstanceType.CLIENT, metrics);
    }
    LOG.debug("Put {} metrics of client {}", metrics.size(), source);
  }

  /**
   * Update the reported metrics received from a worker or client.
   *
   * Cluster metrics of {@link MetricType} COUNTER are directly incremented by the reported values.
   *
   * @param instanceType the instance type that reports the metrics
   * @param reportedMetrics the metrics received by the RPC handler
   */
  private void putReportedMetrics(InstanceType instanceType, List<Metric> reportedMetrics) {
    for (Metric metric : reportedMetrics) {
      if (metric.getSource() == null) {
        continue; // ignore metrics whose source is null
      }

      // If a metric is COUNTER, the value sent via RPC should be the incremental value; i.e.
      // the amount the value has changed since the last RPC. The master should equivalently
      // increment its value based on the received metric rather than replacing it.
      if (metric.getMetricType() == MetricType.COUNTER) {
        ClusterCounterKey key = new ClusterCounterKey(instanceType, metric.getName());
        Counter counter = mClusterCounters.get(key);
        if (counter != null) {
          counter.inc((long) metric.getValue());
          continue;
        }
        if (instanceType.equals(InstanceType.CLIENT)) {
          continue;
        }
        // Need to increment two metrics: one for the specific ufs the current metric recorded from
        // and one to summarize values from all UFSes
        if (metric.getName().equals(BYTES_READ_UFS)) {
          incrementUfsRelatedCounters(metric, MetricKey.CLUSTER_BYTES_READ_UFS.getName(),
              MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName());
        } else if (metric.getName().equals(BYTES_WRITTEN_UFS)) {
          incrementUfsRelatedCounters(metric, MetricKey.CLUSTER_BYTES_WRITTEN_UFS.getName(),
              MetricKey.CLUSTER_BYTES_WRITTEN_UFS_ALL.getName());
        }
      }
    }
  }

  /**
   * Increments the related counters of a specific metric.
   *
   * @param metric the metric
   * @param perUfsMetricName the per ufs metric name prefix to increment counter value of
   * @param allUfsMetricName the all ufs metric name to increment counter value of
   */
  private void incrementUfsRelatedCounters(Metric metric,
      String perUfsMetricName, String allUfsMetricName) {
    String fullCounterName = Metric.getMetricNameWithTags(perUfsMetricName,
        MetricInfo.TAG_UFS, metric.getTags().get(MetricInfo.TAG_UFS));
    Counter perUfsCounter = mClusterCounters.computeIfAbsent(
        new ClusterCounterKey(InstanceType.CLUSTER, fullCounterName),
        n -> MetricsSystem.counter(fullCounterName));
    long counterValue = (long) metric.getValue();
    perUfsCounter.inc(counterValue);
    mClusterCounters.get(new ClusterCounterKey(InstanceType.CLUSTER, allUfsMetricName))
        .inc(counterValue);
  }

  /**
   * Inits the metrics store.
   * Defines the cluster metrics metrics.
   */
  public void initMetricKeys() {
    try (LockResource r = new LockResource(mLock.readLock())) {
      // worker metrics
      mClusterCounters.putIfAbsent(new ClusterCounterKey(InstanceType.WORKER,
              MetricKey.WORKER_BYTES_READ_DIRECT.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_DIRECT.getName()));
      mClusterCounters.putIfAbsent(new ClusterCounterKey(InstanceType.WORKER,
          MetricKey.WORKER_BYTES_READ_REMOTE.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_REMOTE.getName()));
      mClusterCounters.putIfAbsent(new ClusterCounterKey(InstanceType.WORKER,
          MetricKey.WORKER_BYTES_READ_DOMAIN.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_DOMAIN.getName()));
      mClusterCounters.putIfAbsent(new ClusterCounterKey(InstanceType.WORKER,
          MetricKey.WORKER_BYTES_WRITTEN_REMOTE.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_REMOTE.getName()));
      mClusterCounters.putIfAbsent(new ClusterCounterKey(InstanceType.WORKER,
          MetricKey.WORKER_BYTES_WRITTEN_DOMAIN.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN.getName()));
      mClusterCounters.putIfAbsent(new ClusterCounterKey(InstanceType.WORKER,
          MetricKey.WORKER_ACTIVE_RPC_READ_COUNT.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_ACTIVE_RPC_READ_COUNT.getName()));
      mClusterCounters.putIfAbsent(new ClusterCounterKey(InstanceType.WORKER,
          MetricKey.WORKER_ACTIVE_RPC_WRITE_COUNT.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_ACTIVE_RPC_WRITE_COUNT.getName()));

      // client metrics
      mClusterCounters.putIfAbsent(new ClusterCounterKey(InstanceType.CLIENT,
          MetricKey.CLIENT_BYTES_READ_LOCAL.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_LOCAL.getName()));
      mClusterCounters.putIfAbsent(new ClusterCounterKey(InstanceType.CLIENT,
          MetricKey.CLIENT_BYTES_WRITTEN_LOCAL.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_LOCAL.getName()));

      // special metrics that have multiple worker metrics to summarize from
      // always use the full name instead of metric name for those metrics
      mClusterCounters.putIfAbsent(new ClusterCounterKey(InstanceType.CLUSTER,
          MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName()));
      mClusterCounters.putIfAbsent(new ClusterCounterKey(InstanceType.CLUSTER,
          MetricKey.CLUSTER_BYTES_WRITTEN_UFS_ALL.getName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_ALL.getName()));

      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.CLUSTER_CACHE_HIT_RATE.getName()),
          () -> {
            long cacheMisses = MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName())
                .getCount();
            long total =
                MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_DIRECT.getName()).getCount()
                + MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_REMOTE.getName()).getCount()
                + MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_DOMAIN.getName()).getCount()
                + MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_LOCAL.getName()).getCount();
            if (total > 0) {
              return 1 - cacheMisses / (1.0 * total);
            }
            return 0;
          });
    }
  }

  /**
   * Clears all the metrics.
   *
   * This method should only be called when starting the {@link DefaultMetricsMaster}
   * and before starting the metrics updater to avoid conflicts with
   * other methods in this class which updates or accesses
   * the metrics inside metrics sets.
   */
  public void clear() {
    long start = System.currentTimeMillis();
    try (LockResource r = new LockResource(mLock.writeLock())) {
      for (Counter counter : mClusterCounters.values()) {
        counter.dec(counter.getCount());
      }
      mLastClearTime = mClock.millis();
      MetricsSystem.resetAllMetrics();
    }
    LOG.info("Cleared the metrics store and metrics system in {} ms",
        System.currentTimeMillis() - start);
  }

  /**
   * @return the last metrics store clear time in milliseconds
   */
  public long getLastClearTime() {
    try (LockResource r = new LockResource(mLock.readLock())) {
      return mLastClearTime;
    }
  }

  /**
   * The key for cluster counter map.
   * This class is added to reduce the string concatenation of instancetype + "." + metric name.
   */
  public static class ClusterCounterKey {
    private InstanceType mInstanceType;
    private String mMetricName;

    /**
     * Construct a new {@link ClusterCounterKey}.
     *
     * @param instanceType the instance type
     * @param metricName the metric name
     */
    public ClusterCounterKey(InstanceType instanceType, String metricName) {
      mInstanceType = instanceType;
      mMetricName = metricName;
    }

    /**
     * @return the instance type
     */
    private InstanceType getInstanceType() {
      return mInstanceType;
    }

    /**
     * @return the metric name
     */
    private String getMetricName() {
      return mMetricName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ClusterCounterKey)) {
        return false;
      }
      ClusterCounterKey that = (ClusterCounterKey) o;
      return Objects.equal(mInstanceType, that.getInstanceType())
          && Objects.equal(mMetricName, that.getMetricName());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mInstanceType, mMetricName);
    }

    @Override
    public String toString() {
      return mInstanceType + "." + mMetricName;
    }
  }
}
