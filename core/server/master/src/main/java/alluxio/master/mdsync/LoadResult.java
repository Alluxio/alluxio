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

/**
 * This is the result of a single batch load from the UFS.
 */
class LoadResult {
  private final TaskInfo mTaskInfo;
  private final AlluxioURI mBaseLoadPath;
  private final UfsLoadResult mUfsLoadResult;
  private final long mLoadRequestId;

  LoadResult(
      long loadRequestId, AlluxioURI baseLoadPath, TaskInfo taskInfo, UfsLoadResult ufsLoadResult) {
    mLoadRequestId = loadRequestId;
    mBaseLoadPath = baseLoadPath;
    mTaskInfo = taskInfo;
    mUfsLoadResult = ufsLoadResult;
  }

  AlluxioURI getBaseLoadPath() {
    return mBaseLoadPath;
  }

  public UfsLoadResult getUfsLoadResult() {
    return mUfsLoadResult;
  }

  TaskInfo getTaskInfo() {
    return mTaskInfo;
  }

  void onProcessComplete(SyncProcessResult result) {
    mTaskInfo.getMdSync().onProcessComplete(mTaskInfo.getId(), mLoadRequestId, result);
  }

  void onProcessError(Throwable t) {
    mTaskInfo.getMdSync().onProcessError(mTaskInfo.getId(), t);
  }
}
