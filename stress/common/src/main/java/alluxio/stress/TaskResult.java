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

package alluxio.stress;

import alluxio.util.JsonSerializable;

/**
 * This represents the result of a single stress task. A {@link TaskResult} is meant to be
 * aggregated into a {@link Summary}.
 */
public interface TaskResult extends JsonSerializable {

  /**
   * @return the aggregator that can produce a summary
   */
  Aggregator aggregator();

  /**
   * The interface that aggregates multiple task results into a summary.
   *
   * @param <T> a {@link TaskResult} type
   */
  interface Aggregator<T extends TaskResult> {
    /**
     * Aggregates multiple instances of a {@link TaskResult} to a summary.
     *
     * @param results list of {@link TaskResult}
     * @return the aggregated summary
     */
    Summary aggregate(Iterable<T> results) throws Exception;
  }
}
