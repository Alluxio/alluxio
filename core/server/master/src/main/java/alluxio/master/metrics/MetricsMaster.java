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

import alluxio.grpc.MetricValue;
import alluxio.master.Master;
import alluxio.metrics.Metric;

import java.util.List;
import java.util.Map;

/**
 * Interface of the metrics master that aggregates the cluster-level metrics from workers and
 * clients.
 */
public interface MetricsMaster extends Master {
  /**
   * Clear metrics in the current master.
   */
  void clearMetrics();

  /**
   * Handles the client's heartbeat request for metrics collection.
   *
   * @param source the metrics source
   * @param metrics client-side metrics
   */
  void clientHeartbeat(String source, List<Metric> metrics);

  /**
   * @return the master service handler
   */
  MetricsMasterClientServiceHandler getMasterServiceHandler();

  /**
   * @return all metrics stored in the current master in a map
   *         from metric name to corresponding metric value.
   */
  Map<String, MetricValue> getMetrics();

  /**
   * Handles the worker heartbeat and puts the metrics from an instance with a source name.
   *
   * @param source the source of the metrics
   * @param metrics the new worker metrics
   */
  void workerHeartbeat(String source, List<Metric> metrics);
}
