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

import alluxio.AlluxioURI;
import alluxio.file.options.DescendantType;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * This is a request for a single batch load sent to the UFS.
 */
public class LoadRequest implements Comparable<LoadRequest> {
  private final TaskInfo mTaskInfo;
  private final AlluxioURI mPath;
  private final String mContinuationToken;
  private final DescendantType mDescendantType;
  private final long mId;
  private final AlluxioURI mPreviousLoadLast;
  private final boolean mIsFirstLoad;
  /**
   * This is the id of the load request that started a set of batches of load requests, i.e.
   * the batches of loads until one is not truncated.
   */
  private final long mBatchSetId;
  private final RetryPolicy mRetryPolicy = new CountingRetry(2);

  LoadRequest(
      long id, long batchSetId, TaskInfo taskInfo, AlluxioURI path,
      @Nullable String continuationToken,
      @Nullable AlluxioURI previousLoadLast,
      DescendantType descendantType,
      boolean isFirstLoad) {
    taskInfo.getStats().gotLoadRequest();
    mTaskInfo = taskInfo;
    mPath = path;
    mId = id;
    mBatchSetId = batchSetId;
    mContinuationToken = continuationToken;
    mDescendantType = descendantType;
    mPreviousLoadLast = previousLoadLast;
    mIsFirstLoad = isFirstLoad;
  }

  Optional<AlluxioURI> getPreviousLoadLast() {
    return Optional.ofNullable(mPreviousLoadLast);
  }

  /**
   * @return the batch ID, i.e. the load ID of the directory that initiated this load
   * if using {@link alluxio.file.options.DirectoryLoadType#BFS} or
   * {@link alluxio.file.options.DirectoryLoadType#DFS}
   */
  long getBatchSetId() {
    return mBatchSetId;
  }

  boolean attempt() {
    return mRetryPolicy.attempt();
  }

  /**
   * @return the task info
   */
  TaskInfo getTaskInfo() {
    return mTaskInfo;
  }

  /**
   * @return if the load request is the first load request
   */
  boolean isFirstLoad() {
    return mIsFirstLoad;
  }

  AlluxioURI getLoadPath() {
    return mPath;
  }

  /**
   * @return the descendant type for this specific load request. Note
   * that this may be different from the descendant type of the overall
   * sync operation. For example if the {@link alluxio.file.options.DirectoryLoadType}
   * is BFS or DFS and the overall descendant type is ALL, then the
   * descendant type of each of the load requests will be ONE.
   */
  DescendantType getDescendantType() {
    return mDescendantType;
  }

  long getBaseTaskId() {
    return mTaskInfo.getId();
  }

  /**
   * @return the unique id for this specific load request
   */
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

  @Override
  public int compareTo(LoadRequest o) {
    // First compare the directory load id
    int baseTaskCmp;
    switch (o.mTaskInfo.getLoadByDirectory()) {
      case SINGLE_LISTING:
        return Long.compare(mId, o.mId);
      case DFS:
        baseTaskCmp = Long.compare(o.mBatchSetId, mBatchSetId);
        break;
      default:
        baseTaskCmp = Long.compare(mBatchSetId, o.mBatchSetId);
        break;
    }
    if (baseTaskCmp != 0) {
      return baseTaskCmp;
    }
    // then compare the base id
    return Long.compare(mId, o.mId);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LoadRequest) {
      return compareTo((LoadRequest) obj) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    // fix find bugs
    return super.hashCode();
  }
}
