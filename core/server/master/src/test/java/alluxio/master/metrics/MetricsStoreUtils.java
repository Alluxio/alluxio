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

import alluxio.Constants;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

/**
 * The common utils for testing {@link MetricsStore}.
 */
public class MetricsStoreUtils {
  private static final int WAIT_TIMEOUT = 5 * Constants.SECOND_MS;

  /**
   * Metrics reported by worker or client are processed asynchronous.
   * Waits until all the cached instance metrics are processed.
   *
   * @param metricsStore the metrics store to check
   */
  public static void waitForMetricsCacheQueueEmpty(MetricsStore metricsStore) throws Exception {
    CommonUtils.waitFor("get correct metrics result", () -> {
      try {
        if (metricsStore.getMetricsQueueSize() == 0) {
          return true;
        } else {
          return false;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(WAIT_TIMEOUT));
  }
}
