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
import alluxio.metrics.MetricIdentifier;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.MetricsSystem.InstanceType;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A store of metrics containing the metrics collected from workers and clients.
 */
@ThreadSafe
public class MetricsStore {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsStore.class);
  private static final IndexDefinition<Metric, MetricIdentifier> METRIC_IDENTIFIER_INDEX =
      new IndexDefinition<Metric, MetricIdentifier>(true) {
        @Override
        public MetricIdentifier getFieldValue(Metric o) {
          return o.getMetricIdentifier();
        }
      };

  private static final IndexDefinition<Metric, String> NAME_INDEX =
      new IndexDefinition<Metric, String>(false) {
        @Override
        public String getFieldValue(Metric o) {
          return o.getName();
        }
      };

  // Although IndexedSet is threadsafe, it lacks an update operation, so we need locking to
  // implement atomic update using remove + add.
  private final IndexedSet<Metric> mWorkerMetrics =
      new IndexedSet<>(METRIC_IDENTIFIER_INDEX, NAME_INDEX);
  private final IndexedSet<Metric> mClientMetrics =
      new IndexedSet<>(METRIC_IDENTIFIER_INDEX, NAME_INDEX);

  /**
   * A thread-safe linked-list-based queue with an optional capacity limit.
   * If the rate of submitting worker metrics and client metrics are much faster
   * than the rate of processing submitted metrics, no new metrics will be accepted.
   * The stored worker metrics and client metrics will become incorrect.
   */
  private final LinkedBlockingQueue<InstanceMetrics> mMetricsQueue;

  // Dedicated thread for process metrics in the metrics queue.
  private Thread mMetricsProcessThread;
  // Timeout for submitting instance metrics to metrics queue.
  private final long mMetricsSubmitTimeout;

  // Control flag that is used to instruct metrics processing thread to exit.
  private volatile boolean mStopProcessing = false;

  /**
   * Constructs a new {@link MetricsStore}.
   */
  public MetricsStore() {
    mMetricsQueue = new LinkedBlockingQueue<>(ServerConfiguration
        .getInt(PropertyKey.MASTER_METRICS_QUEUE_CAPACITY));
    mMetricsSubmitTimeout = ServerConfiguration
        .getMs(PropertyKey.MASTER_METRICS_QUEUE_OFFER_TIMEOUT);
  }

  /**
   * Gets the full instance id of the concatenation of hostname and the id. The dots in the hostname
   * replaced by underscores.
   *
   * @param hostname the hostname
   * @param id the instance id
   * @return the full instance id of hostname[:id]
   */
  private static String getFullInstanceId(String hostname, String id) {
    String str = hostname == null ? "" : hostname;
    str = str.replace('.', '_');
    str += (id == null ? "" : "-" + id);
    return str;
  }

  /**
   * Put the metrics from a worker with a hostname into the metrics queue.
   * The submitted metrics will be processed asynchronously.
   *
   * @param hostname the hostname of the instance
   * @param metrics the new worker metrics
   */
  public void putWorkerMetrics(String hostname, List<Metric> metrics) {
    if (metrics.isEmpty()) {
      return;
    }
    putMetrics(new InstanceMetrics(hostname, null, metrics));
  }

  /**
   * Put the metrics from a client with a hostname and a client id into the metric queue.
   * The submitted metrics will be processed asynchronously.
   *
   * @param hostname the hostname of the client
   * @param clientId the id of the client
   * @param metrics the new metrics
   */
  public void putClientMetrics(String hostname, String clientId, List<Metric> metrics) {
    if (metrics.isEmpty()) {
      return;
    }
    putMetrics(new InstanceMetrics(hostname, clientId, metrics));
  }

  /**
   * Put the instance metrics into the metrics queue.
   * If the put process fail, error message will be logged instead of throwing exception
   * to affect master process.
   *
   * @param instanceMetrics the instance metrics to put
   */
  private void putMetrics(InstanceMetrics instanceMetrics) {
    try {
      boolean submitted = mMetricsQueue.offer(instanceMetrics,
          mMetricsSubmitTimeout, TimeUnit.MILLISECONDS);
      if (!submitted) {
        LOG.error("Master metrics queue is full, fail to submit metrics of {}",
            instanceMetrics.getHostname());
      }
    } catch (InterruptedException e) {
      LOG.error("Submitting metrics of hostname {} is interrupted", instanceMetrics.getHostname());
    }
  }

  /**
   * A dedicated thread that goes over outstanding queue items and process them.
   */
  private void processMetrics() {
    while (!mStopProcessing) {
      while (!mMetricsQueue.isEmpty() && !mStopProcessing) {
        InstanceMetrics submittedMetrics = mMetricsQueue.poll();
        processSingleInstanceMetrics(submittedMetrics);
      }
    }
  }

  /**
   * Update the worker or client metrics with the given instance metrics.
   *
   * Any metrics from the given instanceId which are not reported in the new set of metrics are
   * removed. Metrics of {@link MetricType} COUNTER are incremented by the reported values.
   * Otherwise, all metrics are simply replaced.
   *
   * @param instanceMetrics The single instance metrics to be processed
   */
  private void processSingleInstanceMetrics(InstanceMetrics instanceMetrics) {
    IndexedSet<Metric> metricSet = instanceMetrics.getId() == null
        ? mWorkerMetrics : mClientMetrics;
    List<Metric> reportedMetrics = instanceMetrics.getMetricsList();
    for (Metric metric : reportedMetrics) {
      if (metric.getHostname() == null) {
        continue; // ignore metrics whose hostname is null
      }

      Metric oldMetric = metricSet.getFirstByField(METRIC_IDENTIFIER_INDEX,
          metric.getMetricIdentifier());
      if (oldMetric == null) {
        metricSet.add(metric);
      } else if (metric.getMetricType() == MetricType.COUNTER) {
        oldMetric.addValue(metric.getValue());
      } else {
        oldMetric.setValue(metric.getValue());
      }
    }
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
    if (mStopProcessing) {
      return new HashSet<>();
    }
    if (instanceType == InstanceType.MASTER) {
      return getMasterMetrics(name);
    }

    if (instanceType == InstanceType.WORKER) {
      synchronized (mWorkerMetrics) {
        return mWorkerMetrics.getByField(NAME_INDEX, name);
      }
    } else if (instanceType == InstanceType.CLIENT) {
      synchronized (mClientMetrics) {
        return mClientMetrics.getByField(NAME_INDEX, name);
      }
    } else {
      throw new IllegalArgumentException("Unsupported instance type " + instanceType);
    }
  }

  private Set<Metric> getMasterMetrics(String name) {
    Set<Metric> metrics = new HashSet<>();
    for (Metric metric : MetricsSystem.allMasterMetrics()) {
      if (metric.getName().equals(name)) {
        metrics.add(metric);
      }
    }
    return metrics;
  }

  /**
   * Starts processing metrics.
   */
  public synchronized void start() {
    if (mMetricsProcessThread != null) {
      stop();
    }
    // Clears all the existing metrics to support master failover
    if (!mWorkerMetrics.isEmpty()) {
      mWorkerMetrics.clear();
    }
    if (!mClientMetrics.isEmpty()) {
      mClientMetrics.clear();
    }
    mMetricsQueue.clear();

    // Create a new thread.
    mMetricsProcessThread = new Thread(this::processMetrics, "MetricsProcessingThread");
    // Reset termination flag before starting the new thread.
    mMetricsProcessThread.start();
    mStopProcessing = false;
  }

  /**
   * Stops processing metrics.
   */
  public synchronized void stop() {
    mStopProcessing = true;

    if (mMetricsProcessThread != null) {
      try {
        mMetricsProcessThread.join();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      } finally {
        mMetricsProcessThread = null;
      }
    }
  }

  /**
   * @return the metrics caching queue size
   */
  @VisibleForTesting
  protected int getMetricsQueueSize() {
    return mMetricsQueue.size();
  }

  /**
   * The instance metrics that represent the metrics that submitted by a worker or a client.
   */
  private class InstanceMetrics {
    private String mHostname;
    private String mId;
    private List<Metric> mMetrics;

    public InstanceMetrics(String hostname, String id, List<Metric> metrics) {
      mHostname = hostname;
      mId = id;
      mMetrics = metrics;
    }

    public String getHostname() {
      return mHostname;
    }

    public String getId() {
      return mId;
    }

    public List<Metric> getMetricsList() {
      return mMetrics;
    }
  }
}
