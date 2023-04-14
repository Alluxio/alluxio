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
import alluxio.underfs.UfsLoadResult;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * This is the result of a single batch load from the UFS.
 */
public class LoadResult {
  private final TaskInfo mTaskInfo;
  private final AlluxioURI mBaseLoadPath;
  private final UfsLoadResult mUfsLoadResult;
  private final LoadRequest mLoadRequest;
  private final AlluxioURI mPreviousLast;
  private final boolean mIsFirstLoad;

  public LoadResult(
      LoadRequest loadRequest, AlluxioURI baseLoadPath, TaskInfo taskInfo,
      @Nullable AlluxioURI previousLast, UfsLoadResult ufsLoadResult,
      boolean isFirstLoad) {
    mLoadRequest = loadRequest;
    mBaseLoadPath = baseLoadPath;
    mTaskInfo = taskInfo;
    mUfsLoadResult = ufsLoadResult;
    mPreviousLast = previousLast;
    mIsFirstLoad = isFirstLoad;
  }

  public boolean isFirstLoad() {
    return mIsFirstLoad;
  }

  public Optional<AlluxioURI> getPreviousLast() {
    return Optional.ofNullable(mPreviousLast);
  }

  public AlluxioURI getBaseLoadPath() {
    return mBaseLoadPath;
  }

  public UfsLoadResult getUfsLoadResult() {
    return mUfsLoadResult;
  }

  public TaskInfo getTaskInfo() {
    return mTaskInfo;
  }

  void onProcessComplete(SyncProcessResult result) {
    mTaskInfo.getMdSync().onProcessComplete(mTaskInfo.getId(), mLoadRequest.getLoadRequestId(), result);
  }

  void onProcessError(Throwable t) {
    mTaskInfo.getMdSync().onProcessError(mTaskInfo.getId(), t);
  }

  /**
   * @return the load request
   */
  public LoadRequest getLoadRequest() {
    return mLoadRequest;
  }
}
