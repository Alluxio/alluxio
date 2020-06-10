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

package alluxio.stress.client;

import alluxio.stress.JsonSerializable;
import alluxio.stress.common.SummaryStatistics;

import java.util.HashMap;
import java.util.Map;

/**
 * Time to first byte statistics used in {@link ClientIOTaskResult}.
 */
public class TimeToFirstByteStatistics implements JsonSerializable {
  private Map<String, SummaryStatistics> mSummaryStatistics;

  /**
   * Create an instance.
   */
  public TimeToFirstByteStatistics() {
    // Default constructor required for json deserialization
    mSummaryStatistics = new HashMap<>();
  }

  /**
   * @return summary statistics for methods
   */
  public Map<String, SummaryStatistics> getSummaryStatistics() {
    return mSummaryStatistics;
  }

  /**
   * @param statistics summary statistics for methods
   */
  public void setSummaryStatistics(Map<String, SummaryStatistics> statistics) {
    mSummaryStatistics = statistics;
  }

  /**
   * @param method method name
   * @param statistis summary statistics
   */
  public void setSummaryStatistics(String method, SummaryStatistics statistis) {
    mSummaryStatistics.put(method, statistis);
  }
}
