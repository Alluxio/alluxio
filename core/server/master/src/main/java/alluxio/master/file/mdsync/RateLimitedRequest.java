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

package alluxio.master.file.mdsync;

import com.google.common.base.Preconditions;

import java.util.Objects;

class RateLimitedRequest implements Comparable<RateLimitedRequest> {

  PathLoaderTask mTask;
  LoadRequest mLoadRequest;
  long mPermit;

  RateLimitedRequest(PathLoaderTask task, LoadRequest loadRequest, long permit) {
    mTask = Preconditions.checkNotNull(task);
    mLoadRequest = Preconditions.checkNotNull(loadRequest);
    mPermit = permit;
  }

  public boolean isReady() {
    return mTask.getRateLimiter().getWaitTimeNanos(mPermit) <= 0;
  }

  public long getWaitTime() {
    return mTask.getRateLimiter().getWaitTimeNanos(mPermit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RateLimitedRequest that = (RateLimitedRequest) o;
    return mPermit == that.mPermit && mTask.equals(that.mTask)
        && mLoadRequest.equals(that.mLoadRequest);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mTask, mLoadRequest, mPermit);
  }

  @Override
  public int compareTo(RateLimitedRequest o) {
    return Long.compare(mPermit, o.mPermit);
  }
}
