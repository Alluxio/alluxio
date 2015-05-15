/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.metrics.source;

import com.codahale.metrics.MetricRegistry;

/**
 * Source is where the metrics generated. It uses a MetricRegistry to register the metrics for
 * monitoring.
 */
public interface Source {
  /**
   * Get the name of the Source.
   *
   * @return the name of the Source
   */
  public String getName();

  /**
   * Get the instance of the MetricRegistry. A MetricRegistry is used to register the metrics, and
   * is passed to a Sink so that the sink knows which metrics to report.
   *
   * @return the instance of the MetricRegistry
   */
  public MetricRegistry getMetricRegistry();
}
