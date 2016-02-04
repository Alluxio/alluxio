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

package alluxio.retry;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

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
      mCount ++;
      return true;
    }
    return false;
  }
}
