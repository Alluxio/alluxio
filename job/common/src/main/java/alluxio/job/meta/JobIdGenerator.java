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

package alluxio.job.meta;

import alluxio.util.CommonUtils;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class generates unique job ids.
 */
@ThreadSafe
public final class JobIdGenerator {
  private final AtomicLong mNextJobId;

  /**
   * Creates a new instance.
   */
  public JobIdGenerator() {
    // shift by 10,000 to avoid conflicts with worker IDs.
    mNextJobId = new AtomicLong(CommonUtils.getCurrentMs() + 10 * 1000);
  }

  /**
   * @return a new job id
   */
  public long getNewJobId() {
    return mNextJobId.getAndIncrement();
  }
}
