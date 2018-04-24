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

package alluxio.metrics.source;

import com.codahale.metrics.MetricRegistry;

/**
 * Source is where the metrics generated. It uses a {@link MetricRegistry} to register the metrics
 * for monitoring.
 */
public interface Source {
  /**
   * Gets the name of the Source.
   *
   * @return the name of the Source
   */
  String getName();

  /**
   * Gets the instance of the {@link MetricRegistry}. A MetricRegistry is used to register the
   * metrics, and is passed to a Sink so that the sink knows which metrics to report.
   *
   * @return the instance of the MetricRegistry
   */
  MetricRegistry getMetricRegistry();
}
