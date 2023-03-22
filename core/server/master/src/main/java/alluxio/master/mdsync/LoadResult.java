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

import java.util.function.Consumer;

/**
 * This is the result of a single batch load from the UFS.
 */
class LoadResult {
  private final TaskInfo mTaskInfo;
  private final AlluxioURI mBaseLoadPath;
  private final UfsLoadResult mUfsLoadResult;
  private final Consumer<Throwable> mOnError;
  private final Consumer<SyncProcessResult> mOnComplete;

  LoadResult(
      AlluxioURI baseLoadPath, TaskInfo taskInfo, UfsLoadResult ufsLoadResult,
      Consumer<SyncProcessResult> onComplete,
      Consumer<Throwable> onError) {
    mBaseLoadPath = baseLoadPath;
    mTaskInfo = taskInfo;
    mUfsLoadResult = ufsLoadResult;
    mOnError = onError;
    mOnComplete = onComplete;
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

  void onComplete(SyncProcessResult result) {
    if (mOnComplete != null) {
      mOnComplete.accept(result);
    }
  }

  void onProcessError(Throwable t) {
    mOnError.accept(t);
  }
}
