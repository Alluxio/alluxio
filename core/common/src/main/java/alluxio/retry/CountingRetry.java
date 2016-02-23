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

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An option which allows retrying based on maximum count.
 */
@NotThreadSafe
public class CountingRetry implements RetryPolicy {

  private final int mMaxRetries;
  private int mCount = 0;

  /**
   * Constructs a retry facility which allows max number of retries.
   *
   * @param maxRetries max number of retries
   */
  public CountingRetry(int maxRetries) {
    Preconditions.checkArgument(maxRetries > 0, "Max retries must be a positive number");
    mMaxRetries = maxRetries;
  }

  @Override
  public int getRetryCount() {
    return mCount;
  }

  @Override
  public boolean attemptRetry() {
    if (mMaxRetries > mCount) {
      mCount++;
      return true;
    }
    return false;
  }
}
