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
import alluxio.file.options.DirectoryLoadType;

import javax.annotation.Nullable;

/**
 * This represents the overall metadata sync task information.
 */
public class TaskInfo {
  private final AlluxioURI mBasePath;
  private final AlluxioURI mAlluxioPath;
  private final String mStartAfter;
  private final DescendantType mDescendantType;
  private final long mId;
  private final DirectoryLoadType mLoadByDirectory;
  private final long mSyncInterval;
  private final MdSync mMdSync;
  private final TaskStats mStats;

  TaskInfo(
      MdSync mdSync,
      AlluxioURI ufsPath, // basePath should be without the header/bucket, e.g. no s3://
      AlluxioURI alluxioPath,
      @Nullable String startAfter,
      DescendantType descendantType,
      long syncInterval,
      DirectoryLoadType loadByDirectory, long id) {
    mBasePath = ufsPath;
    mAlluxioPath = alluxioPath;
    mSyncInterval = syncInterval;
    mDescendantType = descendantType;
    mLoadByDirectory = loadByDirectory;
    mId = id;
    mStartAfter = startAfter;
    mMdSync = mdSync;
    mStats = new TaskStats();
  }

  public TaskStats getStats() {
    return mStats;
  }

  public AlluxioURI getAlluxioPath() {
    return mAlluxioPath;
  }

  public long getSyncInterval() {
    return mSyncInterval;
  }

  public boolean hasDirLoadTasks() {
    return mDescendantType == DescendantType.ALL
        && mLoadByDirectory != DirectoryLoadType.SINGLE_LISTING;
  }

  String getStartAfter() {
    return mStartAfter;
  }

  public MdSync getMdSync() {
    return mMdSync;
  }

  public AlluxioURI getBasePath() {
    return mBasePath;
  }

  public long getId() {
    return mId;
  }

  public DirectoryLoadType getLoadByDirectory() {
    return mLoadByDirectory;
  }

  public DescendantType getDescendantType() {
    return mDescendantType;
  }

  public DescendantType getInodeIteratorDescendantType() {
    if (mLoadByDirectory != DirectoryLoadType.SINGLE_LISTING
        && mDescendantType == DescendantType.ALL) {
      return DescendantType.ONE;
    }
    return mDescendantType;
  }

  @Override
  public String toString() {
    return String.format(
        "TaskInfo{Base path: %s, Descendant Type: %s, Id: %d}", mBasePath, mDescendantType, mId);
  }
}
