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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Policy for determining whether retries should be performed, and potentially waiting for some time
 * before the next retry attempt. The way that this interface works is that the logic
 * for delayed retries (retries that sleep) can delay the caller of {@link #attempt()}.
 */
@NotThreadSafe
public interface RetryPolicy {

  /**
   * How many retries have been performed. If no retries have been performed, 0 is returned.
   *
   * @return number of retries performed
   */
  int getAttemptCount();

  /**
   * Waits until it is time to perform the next retry, then returns. Returns false if no further
   * retries should be performed. The first call to this method should never delay the caller, this
   * allow users of the policy to use it in the context of a while-loop.
   *
   * @return whether another retry should be performed
   */
  boolean attempt();
}
