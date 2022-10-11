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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;

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
        if (e instanceof AlluxioRuntimeException && !((AlluxioRuntimeException) e).isRetryable()) {
          throw AlluxioRuntimeException.from(e);
        }
        cause = e;
      }
    }
    throw AlluxioRuntimeException.from(cause);
  }

  /**
   * Gives a default ClientRetry based on config.
   * @return default Client Retry policy based on config
   */
  public static RetryPolicy defaultClientRetry() {
    return ExponentialTimeBoundedRetry.builder()
        .withMaxDuration(Configuration.getDuration(PropertyKey.USER_RPC_RETRY_MAX_DURATION))
        .withInitialSleep(Configuration.getDuration(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS))
        .withMaxSleep(Configuration.getDuration(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS))
        .build();
  }

  /**
   * @return the default worker to master client retry
   */
  public static RetryPolicy defaultWorkerMasterClientRetry() {
    return ExponentialTimeBoundedRetry.builder()
        .withMaxDuration(Configuration.getDuration(PropertyKey.WORKER_MASTER_CONNECT_RETRY_TIMEOUT))
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
