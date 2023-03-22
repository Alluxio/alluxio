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

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * This is a request for a single batch load sent to the UFS.
 */
class LoadRequest {
  private final TaskInfo mTaskInfo;
  private final AlluxioURI mPath;
  private final String mContinuationToken;
  private final Consumer<Throwable> mOnError;
  private final Function<UfsLoadResult, Optional<LoadResult>> mOnComplete;
  private final DescendantType mDescendantType;
  private final long mId;

  LoadRequest(
      long id, TaskInfo taskInfo, AlluxioURI path, @Nullable String continuationToken,
      DescendantType descendantType, Function<UfsLoadResult, Optional<LoadResult>> onComplete,
      Consumer<Throwable> onError) {
    mTaskInfo = taskInfo;
    mPath = path;
    mId = id;
    mContinuationToken = continuationToken;
    mOnComplete = onComplete;
    mOnError = onError;
    mDescendantType = descendantType;
  }

  AlluxioURI getLoadPath() {
    return mPath;
  }

  DescendantType getDescendantType() {
    return mDescendantType;
  }

  long getLoadTaskId() {
    return mTaskInfo.getId();
  }

  long getId() {
    return mId;
  }

  @Nullable
  String getContinuationToken() {
    return mContinuationToken;
  }

  Optional<LoadResult> onComplete(UfsLoadResult result) {
    return mOnComplete.apply(result);
  }

  void onError(Throwable t) {
    mOnError.accept(t);
  }
}
