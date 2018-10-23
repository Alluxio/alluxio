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
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.MetricsSystem.InstanceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A store of metrics containing the metrics collected from workers and clients.
 */
@ThreadSafe
public class MetricsStore {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsStore.class);
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
    String str = hostname == null ? "" : hostname;
    str = str.replace('.', '_');
    str += (id == null ? "" : "-" + id);
    return str;
  }

  private final IndexedSet<Metric> mWorkerMetrics =
      new IndexedSet<>(FULL_NAME_INDEX, NAME_INDEX, ID_INDEX);
  private final IndexedSet<Metric> mClientMetrics =
      new IndexedSet<>(FULL_NAME_INDEX, NAME_INDEX, ID_INDEX);

  /**
   * Put the metrics from a worker with a hostname. If all the old metrics associated with this
   * instance will be removed and then replaced by the latest.
   *
   * @param hostname the hostname of the instance
   * @param metrics the new worker metrics
   */
  public synchronized void putWorkerMetrics(String hostname, List<Metric> metrics) {
    if (metrics.isEmpty()) {
      return;
    }
    mWorkerMetrics.removeByField(ID_INDEX, getFullInstanceId(hostname, null));
    for (Metric metric : metrics) {
      if (metric.getHostname() == null) {
        continue; // ignore metrics whose hostname is null
      }
      mWorkerMetrics.add(metric);
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
  public synchronized void putClientMetrics(String hostname, String clientId,
      List<Metric> metrics) {
    if (metrics.isEmpty()) {
      return;
    }
    mClientMetrics.removeByField(ID_INDEX, getFullInstanceId(hostname, clientId));
    for (Metric metric : metrics) {
      if (metric.getHostname() == null) {
        continue; // ignore metrics whose hostname is null
      }
      mClientMetrics.add(metric);
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
  public Set<Metric> getMetricsByInstanceTypeAndName(MetricsSystem.InstanceType instanceType,
      String name) {
    if (instanceType == InstanceType.MASTER) {
      return getMasterMetrics(name);
    }

    if (instanceType == InstanceType.WORKER) {
      synchronized (mWorkerMetrics) {
        return mWorkerMetrics.getByField(NAME_INDEX, name);
      }
    } else if (instanceType == InstanceType.CLIENT) {
      synchronized (mWorkerMetrics) {
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
   * Clears all the metrics.
   */
  public synchronized void clear() {
    mWorkerMetrics.clear();
    mClientMetrics.clear();
  }
}
