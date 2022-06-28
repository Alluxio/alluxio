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

import alluxio.exception.AlluxioRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;

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
   * Notice we assume the exception with the method is retryable, so we are not wasting time here.
   * Retries the given retryable method until it succeed or the retry policy expires. If the retry
   * policy expires, the last exception generated will be rethrown as a AlluxioRuntimeException.
   *
   * @param description a description of the function
   * @param f the function to retry which returns value
   * @param policy the retry policy to use
   * @param <V> result type returned by callable
   * @return callable result
   */
  public static <V> V retryCallable(String description, Callable<V> f, RetryPolicy policy) {
    Exception cause = null;
    while (policy.attempt()) {
      try {
        return f.call();
      } catch (Exception e) {
        LOG.warn("Failed to {} (attempt {}): {}", description, policy.getAttemptCount(),
            e.toString());
        cause = e;
      }
    }
    throw AlluxioRuntimeException.from(cause);
  }

  /**
   * Gives a ClientRetry based on the given parameters.
   *
   * @param maxRetryDuration the maximum total duration to retry for
   * @param baseSleepMs initial sleep time in milliseconds
   * @param maxSleepMs max sleep time in milliseconds
   * @return the default client retry
   */
  public static RetryPolicy defaultClientRetry(Duration maxRetryDuration, Duration baseSleepMs,
      Duration maxSleepMs) {
    return ExponentialTimeBoundedRetry.builder()
        .withMaxDuration(maxRetryDuration)
        .withInitialSleep(baseSleepMs)
        .withMaxSleep(maxSleepMs)
        .build();
  }

  /**
   * @param workerMasterConnectRetryTimeout the max duration to wait between retrying for worker
   *                                        and master
   * @return the default worker to master client retry
   */
  public static RetryPolicy defaultWorkerMasterClientRetry(
      Duration workerMasterConnectRetryTimeout) {
    return ExponentialTimeBoundedRetry.builder()
        .withMaxDuration(workerMasterConnectRetryTimeout)
        .withInitialSleep(Duration.ofMillis(100))
        .withMaxSleep(Duration.ofSeconds(5))
        .build();
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
