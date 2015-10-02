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

package tachyon.client.file.options;

import java.util.concurrent.TimeUnit;

import tachyon.Constants;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;

@PublicApi
public class WaitCompletedOptions {
  public static class Builder {
    private long mTimeout;
    private TimeUnit mTunit;
    private long mPollPeriodMillis;

    /**
     * Creates a new builder for {@link WaitCompletedOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mTimeout = -1;
      mTunit = TimeUnit.MILLISECONDS;
      mPollPeriodMillis = conf.getLong(Constants.USER_WAITCOMPLETED_POLL);
    }


    /**
     * @param timeout the length of the time period after which a thread blocked on a
     *                waitCompleted() call should awake. The time unit of this value is
     *                determined by the other {@code tunit} parameter
     * @param tunit   the time unit of the {@code timeout} parameter.
     * @return the builder
     */
    public Builder setTimeout(long timeout, TimeUnit tunit) {
      mTimeout = timeout;
      mTunit = tunit;
      return this;
    }


    /**
     * Builds a new instance of {@code WaitCompletedOptions}.
     *
     * @return a {@code WaitCompletedOptions} instance
     */
    public WaitCompletedOptions build() {
      return new WaitCompletedOptions(this);
    }
  }

  /**
   * @return the default {@code MkdirOptions}
   */
  public static WaitCompletedOptions defaults() {
    return new Builder(ClientContext.getConf()).build();
  }

  private final long mPollPeriodMillis;
  private final long mTimeout;
  private final TimeUnit mTunit;

  private WaitCompletedOptions(WaitCompletedOptions.Builder builder) {
    mTimeout = builder.mTimeout;
    mTunit = builder.mTunit;
    mPollPeriodMillis = builder.mPollPeriodMillis;
  }

  /**
   * @return the period in milliseconds that the client should use to poll the master
   * about a file completion status
   */
  public long getPollPeriodMillis() {
    return mPollPeriodMillis;
  }

  /**
   *
   * @return the amount of time a client thread should be blocked on a waitCompleted call
   * before being timed out. The time unit of this value is specified by {@link #getTunit()}
   */
  public long getTimeout() {
    return mTimeout;
  }

  /**
   *
   * @return the TimeUnit that should be used to interpret the value returned by
   * {@link #getTimeout()}
   */
  public TimeUnit getTunit() {
    return mTunit;
  }


}
