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

package alluxio.retry;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

/**
 * Utilities for performing retries.
 */
public final class RetryUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RetryUtils.class);

  /**
   * Retries the given method until it doesn't throw an IO exception or the retry policy expires. If
   * the retry policy expires, the last exception generated will be rethrown.
   *
   * @param action a description of the action that fits the phrase "Failed to ${action}"
   * @param f the function to retry
   * @param policy the retry policy to use
   */
  public static void retry(String action, RunnableThrowsIOException f, RetryPolicy policy)
      throws IOException {
    IOException e = null;
    while (policy.attempt()) {
      try {
        f.run();
        return;
      } catch (IOException ioe) {
        e = ioe;
        LOG.warn("Failed to {} (attempt {}): {}", action, policy.getAttemptCount(), e.toString());
      }
    }
    throw e;
  }

  /**
   * @return the default client retry
   */
  public static RetryPolicy defaultClientRetry() {
    Duration maxRetryDuration = Configuration.getDuration(PropertyKey.USER_RPC_RETRY_MAX_DURATION);
    Duration baseSleepMs = Configuration.getDuration(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS);
    Duration maxSleepMs = Configuration.getDuration(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);
    return ExponentialTimeBoundedRetry.builder()
        .withMaxDuration(maxRetryDuration)
        .withInitialSleep(baseSleepMs)
        .withMaxSleep(maxSleepMs)
        .build();
  }

  /**
   * @return the default worker to master client retry
   */
  public static RetryPolicy defaultWorkerMasterClientRetry() {
    return ExponentialTimeBoundedRetry.builder()
        .withMaxDuration(Duration
            .ofMillis(Configuration.getMs(PropertyKey.WORKER_MASTER_CONNECT_RETRY_TIMEOUT)))
        .withInitialSleep(Duration.ofMillis(100))
        .withMaxSleep(Duration.ofSeconds(5))
        .build();
  }

  /**
   * @return the default metrics client retry
   */
  public static RetryPolicy defaultMetricsClientRetry() {
    // No retry for metrics since they are best effort and automatically retried with the heartbeat.
    return new CountingRetry(0);
  }

  /**
   * Interface for methods which return nothing and may throw IOException.
   */
  @FunctionalInterface
  public interface RunnableThrowsIOException {
    /**
     * Runs the runnable.
     */
    void run() throws IOException;
  }

  private RetryUtils() {} // prevent instantiation
}
