/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.retry;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Attempts to retry code from a do/while loop. The way that this interface works is that the logic
 * for delayed retries (retries that sleep) can delay the caller of {@link #attemptRetry()}. Because
 * of this, its best to put retries in do/while loops to avoid the first wait.
 */
@NotThreadSafe
public interface RetryPolicy {

  /**
   * How many retries have been performed. If no retries have been performed, 0 is returned.
   *
   * @return number of retries performed
   */
  int getRetryCount();

  /**
   * Attempts to run the given operation, returning false if unable to (max retries have happened).
   *
   * @return whether the operation have succeeded or failed (max retries have happened)
   */
  boolean attemptRetry();
}
