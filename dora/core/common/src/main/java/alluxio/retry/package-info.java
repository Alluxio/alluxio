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
/**
 * Set of utilities for working with retryable operations. The main entrypoint is
 * {@link alluxio.retry.RetryPolicy} which is designed to work with while loops.
 * <p>
 * Example
 * </p>
 * <pre>
 * {
 *   &#064;code
 *   RetryPolicy retry = new ExponentialBackoffRetry(50, Constants.SECOND_MS, MAX_CONNECT_TRY);
 *   while (retry.attempt()) {
 *     // work to retry
 *   }
 * }
 * </pre>
 */

package alluxio.retry;

