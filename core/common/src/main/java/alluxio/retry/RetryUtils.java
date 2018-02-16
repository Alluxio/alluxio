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

import java.io.IOException;

/**
 * Utilities for performing retries.
 */
public final class RetryUtils {

  /**
   * Retries the given method until it doesn't throw an IO exception or the retry policy expires. If
   * the retry policy expires, the last exception generated will be rethrown.
   *
   * @param f the function to retry
   * @param policy the retry policy to use
   * @return the value returned by the function
   */
  public static void retry(RunnableThrowsIOException f, RetryPolicy policy) throws IOException {
    while (true) {
      try {
        f.run();
        return;
      } catch (IOException e) {
        if (!policy.attemptRetry()) {
          throw e;
        }
      }
    }
  }

  /**
   * Interface for methods which return nothing and may throw IOException.
   */
  @FunctionalInterface
  public interface RunnableThrowsIOException {
    void run() throws IOException;
  }

  public RetryUtils() {} // prevent instantiation
}
