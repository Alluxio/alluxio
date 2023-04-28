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

package alluxio.refresh;

import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A refresh policy which allows refreshing after a specified timeout is reached.
 */
@NotThreadSafe
public class TimeoutRefresh implements RefreshPolicy {

  private final long mRefreshPeriodMs;
  private long mLastAttempTimeMs = 0;
  private boolean mFirstAttempt = true;

  /**
   * Constructs a refresh facility which allows refreshing after a specified timeout is reached.
   *
   * @param refreshPeriodMs period of time to refresh after, in milliseconds
   */
  public TimeoutRefresh(long refreshPeriodMs) {
    Preconditions.checkArgument(refreshPeriodMs > 0, "Retry timeout must be a positive number");
    mRefreshPeriodMs = refreshPeriodMs;
  }

  @Override
  public boolean attempt() {
    if (mFirstAttempt || (CommonUtils.getCurrentMs() - mLastAttempTimeMs) > mRefreshPeriodMs) {
      mLastAttempTimeMs = CommonUtils.getCurrentMs();
      mFirstAttempt = false;
      return true;
    }
    return false;
  }
}
