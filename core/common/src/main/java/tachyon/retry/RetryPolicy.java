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

package tachyon.retry;

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
