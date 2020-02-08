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

import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final IndexDefinition<Metric, String> FULL_NAME_INDEX =
      new IndexDefinition<Metric, String>(true) {
        @Override
        public String getFieldValue(Metric o) {
          return o.getFullMetricName();
        }
      };

  private static final IndexDefinition<Metric, String> NAME_INDEX =
      new IndexDefinition<Metric, String>(false) {
        @Override
        public String getFieldValue(Metric o) {
          return o.getName();
        }
      };

  private static final IndexDefinition<Metric, String> ID_INDEX =
      new IndexDefinition<Metric, String>(false) {
        @Override
        public String getFieldValue(Metric o) {
          return getFullInstanceId(o.getHostname(), o.getInstanceId());
        }
      };

  /**
   * Gets the full instance id of the concatenation of hostname and the id. The dots in the hostname
   * replaced by underscores.
   *
   * @param hostname the hostname
   * @param id the instance id
   * @return the full instance id of hostname[:id]
   */
  private static String getFullInstanceId(String hostname, String id) {
    hostname = hostname.replace('.', '_');
    if (id != null) {
      hostname += Metric.ID_SEPARATOR + id;
    }
    return hostname;
  }

  // The lock to guarantee that only one thread is clearing the metrics
  // and no other interaction with worker/client metrics set is allowed during metrics clearing
  private final ReentrantReadWriteLock mLock = new ReentrantReadWriteLock();

  // Although IndexedSet is threadsafe, it lacks an update operation, so we need locking to
  // implement atomic update using remove + add.
  @GuardedBy("mLock")
  private final IndexedSet<Metric> mWorkerMetrics =
      new IndexedSet<>(FULL_NAME_INDEX, NAME_INDEX, ID_INDEX);
  @GuardedBy("mLock")
  private final IndexedSet<Metric> mClientMetrics =
      new IndexedSet<>(FULL_NAME_INDEX, NAME_INDEX, ID_INDEX);
  // A map from the full instance id (hostname + client id if exists)
  // to its last reported time
  @GuardedBy("mLock")
  private final ConcurrentHashMap<String, Long> mLastReportedTimeMap = new ConcurrentHashMap<>();
  private final long mCleanupAge
      = ServerConfiguration.getMs(PropertyKey.MASTER_REPORTED_METRICS_CLEANUP_AGE);

  /**
   * A map from the cluster counter key representing the metrics to be aggregated
   * to the corresponding aggregated cluster Counter.
   * For example, Counter of cluster.BytesReadAlluxio is aggregated from
   * the worker reported worker.BytesReadAlluxio.
   *
   * Exceptions are the BytesRead/WrittenUfs metrics which records
   * the actual cluster metrics name to its Counter directly.
   */
  @GuardedBy("mLock")
  private final ConcurrentHashMap<ClusterCounterKey, Counter> mClusterCounters
      = new ConcurrentHashMap<>();

  /**
   * Put the metrics from a worker with a hostname. If all the old metrics associated with this
   * instance will be removed and then replaced by the latest.
   *
   * @param hostname the hostname of the instance
   * @param metrics the new worker metrics
   */
  public void putWorkerMetrics(String hostname, List<Metric> metrics) {
    if (metrics.isEmpty() || hostname == null) {
      return;
    }
    try (LockResource r = new LockResource(mLock.readLock())) {
      mLastReportedTimeMap.put(getFullInstanceId(hostname, null), System.currentTimeMillis());
      putReportedMetrics(InstanceType.WORKER, metrics);
    }
  }

  /**
   * Put the metrics from a client with a hostname and a client id. If all the old metrics
   * associated with this instance will be removed and then replaced by the latest.
   *
   * @param hostname the hostname of the client
   * @param clientId the id of the client
   * @param metrics the new metrics
   */
  public void putClientMetrics(String hostname, String clientId, List<Metric> metrics) {
    if (metrics.isEmpty() || hostname == null) {
      return;
    }
    try (LockResource r = new LockResource(mLock.readLock())) {
      mLastReportedTimeMap.put(getFullInstanceId(hostname, clientId), System.currentTimeMillis());
      putReportedMetrics(InstanceType.CLIENT, metrics);
    }
  }

  /**
   * Update the reported metrics received from a
   * worker or client.
   *
   * Cluster metrics of {@link MetricType} COUNTER are directly incremented by the reported values.
   * All other metrics are recorded in the metrics map individually, calculated periodically,
   * and deleted when the report instance doesn't report for a period of time.
   *
   * @param instanceType the instance type that reports the metrics
   * @param reportedMetrics the metrics received by the RPC handler
   */
  private void putReportedMetrics(InstanceType instanceType, List<Metric> reportedMetrics) {
    IndexedSet<Metric> metricSet = mWorkerMetrics;
    if (instanceType.equals(InstanceType.CLIENT)) {
      metricSet = mClientMetrics;
    }
    for (Metric metric : reportedMetrics) {
      if (metric.getHostname() == null) {
        continue; // ignore metrics whose hostname is null
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
      } else if (!metricSet.add(metric)) {
        metricSet.getFirstByField(FULL_NAME_INDEX, metric.getFullMetricName())
            .setValue(metric.getValue());
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
   * Gets all the metrics by instance type and the metric name. The supported instance types are
   * worker and client.
   *
   * @param instanceType the instance type
   * @param name the metric name
   * @return the set of matched metrics
   */
  public Set<Metric> getMetricsByInstanceTypeAndName(
      MetricsSystem.InstanceType instanceType, String name) {
    try (LockResource r = new LockResource(mLock.readLock())) {
      if (instanceType == InstanceType.MASTER) {
        return getMasterMetrics(name);
      }
      if (instanceType == InstanceType.WORKER) {
        return mWorkerMetrics.getByField(NAME_INDEX, name);
      } else if (instanceType == InstanceType.CLIENT) {
        return mClientMetrics.getByField(NAME_INDEX, name);
      } else {
        throw new IllegalArgumentException("Unsupported instance type " + instanceType);
      }
    }
  }

  private Set<Metric> getMasterMetrics(String name) {
    return MetricsSystem.getMasterMetric(name);
  }

  /**
   * Inits the metrics store.
   * Defines the cluster metrics counters.
   */
  public void init() {
    try (LockResource r = new LockResource(mLock.readLock())) {
      // worker metrics
      mClusterCounters.put(new ClusterCounterKey(InstanceType.WORKER,
          MetricKey.WORKER_BYTES_READ_ALLUXIO.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_ALLUXIO.getName()));
      mClusterCounters.put(new ClusterCounterKey(InstanceType.WORKER,
          MetricKey.WORKER_BYTES_READ_DOMAIN.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_DOMAIN.getName()));
      mClusterCounters.put(new ClusterCounterKey(InstanceType.WORKER,
          MetricKey.WORKER_BYTES_WRITTEN_ALLUXIO.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_ALLUXIO.getName()));
      mClusterCounters.put(new ClusterCounterKey(InstanceType.WORKER,
          MetricKey.WORKER_BYTES_WRITTEN_DOMAIN.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN.getName()));

      // client metrics
      mClusterCounters.put(new ClusterCounterKey(InstanceType.CLIENT,
          MetricKey.CLIENT_BYTES_READ_LOCAL.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_LOCAL.getName()));
      mClusterCounters.put(new ClusterCounterKey(InstanceType.CLIENT,
          MetricKey.CLIENT_BYTES_WRITTEN_LOCAL.getMetricName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_LOCAL.getName()));

      // special metrics that have multiple worker metrics to summarize from
      // always use the full name instead of metric name for those metrics
      mClusterCounters.put(new ClusterCounterKey(InstanceType.CLUSTER,
          MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName()));
      mClusterCounters.put(new ClusterCounterKey(InstanceType.CLUSTER,
          MetricKey.CLUSTER_BYTES_WRITTEN_UFS_ALL.getName()),
          MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_ALL.getName()));
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
    try (LockResource r = new LockResource(mLock.writeLock())) {
      mWorkerMetrics.clear();
      mClientMetrics.clear();
      for (Counter counter : mClusterCounters.values()) {
        counter.dec(counter.getCount());
      }
      MetricsSystem.resetAllMetrics();
    }
  }

  /**
   * Clears all the orphaned metrics belonging to worker/client
   * which hasn't reported for a period of time.
   */
  public void cleanUpOrphanedMetrics() {
    try (LockResource r = new LockResource(mLock.writeLock())) {
      long current = System.currentTimeMillis();
      Iterator<Map.Entry<String, Long>> iterator = mLastReportedTimeMap.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, Long> entry = iterator.next();
        if (current - entry.getValue() > mCleanupAge) {
          if (entry.getKey().contains(Metric.ID_SEPARATOR)) {
            mClientMetrics.removeByField(ID_INDEX, entry.getKey());
          } else {
            mWorkerMetrics.removeByField(ID_INDEX, entry.getKey());
          }
          iterator.remove();
        }
      }
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
