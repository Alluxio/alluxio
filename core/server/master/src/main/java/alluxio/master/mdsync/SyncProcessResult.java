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
import alluxio.master.file.metasync.SyncResult;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * This is the result of performing the metadata sync in Alluxio.
 */
public class SyncProcessResult {

  private final AlluxioURI mBaseLoadPath;
  private final TaskInfo mTaskInfo;
  private final PathSequence mLoaded;
  private final boolean mIsTruncated;
  private final boolean mRootPathIsFile;
  private final SyncResult mSyncResult;
  private final boolean mIsFirstLoad;

  public SyncProcessResult(
      TaskInfo taskInfo, AlluxioURI baseLoadPath,
      @Nullable PathSequence loaded, boolean isTruncated,
      boolean rootPathIsFile, SyncResult result,
      boolean isFirstLoad) {
    mRootPathIsFile = rootPathIsFile;
    mBaseLoadPath = baseLoadPath;
    mTaskInfo = taskInfo;
    mLoaded = loaded;
    mIsTruncated = isTruncated;
    mSyncResult = result;
    mIsFirstLoad = isFirstLoad;
  }

  public boolean isFirstLoad() {
    return mIsFirstLoad;
  }

  public SyncResult getSyncResult() {
    return mSyncResult;
  }

  public boolean rootPathIsFile() {
    return mRootPathIsFile;
  }

  public AlluxioURI getBaseLoadPath() {
    return mBaseLoadPath;
  }

  public boolean isTruncated() {
    return mIsTruncated;
  }

  public Optional<PathSequence> getLoaded() {
    return Optional.ofNullable(mLoaded);
  }

  public TaskInfo getTaskInfo() {
    return mTaskInfo;
  }
}
