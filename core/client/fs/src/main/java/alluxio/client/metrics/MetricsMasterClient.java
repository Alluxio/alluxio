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

import alluxio.grpc.ClearMetricsPOptions;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for a metrics master client.
 */
public interface MetricsMasterClient extends Closeable {

  /**
   * Clear the metrics stored in metrics system and metrics store.
   *
   * @param clearMetricsOptions the options to clear metrics
   */
  void clearMetrics(ClearMetricsPOptions clearMetricsOptions) throws IOException;
}
