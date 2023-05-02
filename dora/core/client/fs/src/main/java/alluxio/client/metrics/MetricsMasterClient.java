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

package alluxio.client.metrics;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.ClientMetrics;
import alluxio.grpc.MetricValue;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for a metrics master client.
 */
public interface MetricsMasterClient extends Closeable {

  /**
   * Clear the master metrics.
   */
  void clearMetrics() throws IOException;

  /**
   * The method the worker should periodically execute to heartbeat back to the master.
   *
   * @param metrics a list of client metrics
   */
  void heartbeat(final List<ClientMetrics> metrics) throws IOException;

  /**
   * Gets all the metrics stored in the current master from metric name to metric value.
   *
   * @return a map of metrics information
   */
  Map<String, MetricValue> getMetrics() throws AlluxioStatusException;
}
