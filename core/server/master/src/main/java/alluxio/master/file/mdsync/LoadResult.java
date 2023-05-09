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
import alluxio.underfs.UfsLoadResult;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * This is the result of a single batch load from the UFS.
 */
public class LoadResult implements Comparable<LoadResult> {
  private final TaskInfo mTaskInfo;
  private final AlluxioURI mBaseLoadPath;
  private final UfsLoadResult mUfsLoadResult;
  private final LoadRequest mLoadRequest;
  private final AlluxioURI mPreviousLast;
  private final boolean mIsFirstLoad;

  /**
   * Creates a load result.
   * @param loadRequest the load request
   * @param baseLoadPath the base load path
   * @param taskInfo the task info
   * @param previousLast the previous last load item
   * @param ufsLoadResult the ufs load result
   * @param isFirstLoad if the load is the first load
   */
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

  /**
   * @return true if this is the first load
   */
  public boolean isFirstLoad() {
    return mIsFirstLoad;
  }

  /**
   * @return the last item in the previous load
   */
  public Optional<AlluxioURI> getPreviousLast() {
    return Optional.ofNullable(mPreviousLast);
  }

  /**
   * @return the load path
   */
  public AlluxioURI getBaseLoadPath() {
    return mBaseLoadPath;
  }

  /**
   * @return the ufs load result
   */
  public UfsLoadResult getUfsLoadResult() {
    return mUfsLoadResult;
  }

  /**
   * @return the task info
   */
  public TaskInfo getTaskInfo() {
    return mTaskInfo;
  }

  void onProcessComplete(SyncProcessResult result) {
    mTaskInfo.getMdSync().onProcessComplete(
        mTaskInfo.getId(), mLoadRequest.getLoadRequestId(), result);
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

  @Override
  public int compareTo(LoadResult o) {
    int idCmp = Long.compare(mTaskInfo.getId(), o.mTaskInfo.getId());
    if (idCmp != 0) {
      return idCmp;
    }
    return mLoadRequest.compareTo(o.mLoadRequest);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LoadResult) {
      return compareTo((LoadResult) obj) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    // fix find bugs
    return super.hashCode();
  }
}
