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

package alluxio.master.mdsync;

import alluxio.AlluxioURI;
import alluxio.file.options.DescendantType;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

import javax.annotation.Nullable;

/**
 * This is a request for a single batch load sent to the UFS.
 */
class LoadRequest {
  private final TaskInfo mTaskInfo;
  private final AlluxioURI mPath;
  private final String mContinuationToken;
  private final DescendantType mDescendantType;
  private final long mId;
  /**
   * This is the id of the load request that started a set of batches of load requests, i.e.
   * the batches of loads until one is not truncated.
   */
  private final long mBatchSetId;
  private final RetryPolicy mRetryPolicy = new CountingRetry(2);

  LoadRequest(
      long id, long batchSetId, TaskInfo taskInfo, AlluxioURI path,
      @Nullable String continuationToken, DescendantType descendantType) {
    taskInfo.getStats().gotLoadRequest();
    mTaskInfo = taskInfo;
    mPath = path;
    mId = id;
    mBatchSetId = batchSetId;
    mContinuationToken = continuationToken;
    mDescendantType = descendantType;
  }

  long getBatchSetId() {
    return mBatchSetId;
  }

  boolean attempt() {
    return mRetryPolicy.attempt();
  }

  public TaskInfo getTaskInfo() {
    return mTaskInfo;
  }

  AlluxioURI getLoadPath() {
    return mPath;
  }

  DescendantType getDescendantType() {
    return mDescendantType;
  }

  long getBaseTaskId() {
    return mTaskInfo.getId();
  }

  long getLoadRequestId() {
    return mId;
  }

  @Nullable
  String getContinuationToken() {
    return mContinuationToken;
  }

  void onError(Throwable t) {
    mTaskInfo.getMdSync().onLoadRequestError(mTaskInfo.getId(), mId, t);
  }
}
