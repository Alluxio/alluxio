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

import alluxio.master.Master;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;

import java.util.List;

/**
 * Interface of the metrics master that aggregates the cluster-level metrics from workers and
 * clients.
 */
public interface MetricsMaster extends Master  {
  /**
   * Put the metrics from an instance with a hostname. If all the old metrics associated with this
   * instance will be removed and then replaced by the latest.
   *
   * @param instance the instance type
   * @param hostname the hostname of the instance
   * @param metrics the new worker metrics
   */
  void putWorkerMetrics(MetricsSystem.InstanceType instance, String hostname, List<Metric> metrics);
}
