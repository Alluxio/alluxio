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
  private final long mMountId;
  private final boolean mCheckNestedMount;

  TaskInfo(
      MdSync mdSync,
      AlluxioURI ufsPath, // basePath should be without the header/bucket, e.g. no s3://
      AlluxioURI alluxioPath,
      @Nullable String startAfter,
      DescendantType descendantType,
      long syncInterval,
      DirectoryLoadType loadByDirectory,
      long id,
      long mountId,
      boolean checkNestedMount) {
    mBasePath = ufsPath;
    mAlluxioPath = alluxioPath;
    mSyncInterval = syncInterval;
    mDescendantType = descendantType;
    mLoadByDirectory = loadByDirectory;
    mId = id;
    mStartAfter = startAfter;
    mMdSync = mdSync;
    mStats = new TaskStats();
    mMountId = mountId;
    mCheckNestedMount = checkNestedMount;
  }

  /**
   * @return the task stats
   */
  public TaskStats getStats() {
    return mStats;
  }

  /**
   * @return the alluxio path
   */
  public AlluxioURI getAlluxioPath() {
    return mAlluxioPath;
  }

  /**
   * @return the sync interval
   */
  public long getSyncInterval() {
    return mSyncInterval;
  }

  /**
   * @return true, if the task contains dir load tasks
   */
  public boolean hasDirLoadTasks() {
    return mDescendantType == DescendantType.ALL
        && mLoadByDirectory != DirectoryLoadType.SINGLE_LISTING;
  }

  String getStartAfter() {
    return mStartAfter;
  }

  /**
   * @return the metadata sync kernel
   */
  public MdSync getMdSync() {
    return mMdSync;
  }

  /**
   * @return the base path
   */
  public AlluxioURI getBasePath() {
    return mBasePath;
  }

  /**
   * @return the id
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the load by directory type
   */
  public DirectoryLoadType getLoadByDirectory() {
    return mLoadByDirectory;
  }

  /**
   * @return the descendant type
   */
  public DescendantType getDescendantType() {
    return mDescendantType;
  }

  /**
   * @return the mount id
   */
  public long getMountId() {
    return mMountId;
  }

  /**
   * @return true if nested mount should be checked during sync processing
   */
  public boolean checkNestedMount() {
    return mCheckNestedMount;
  }

  @Override
  public String toString() {
    return String.format(
        "TaskInfo{UFS path: %s, AlluxioPath: %s, Descendant Type: %s,"
            + " Directory Load Type: %s, Id: %d}", mBasePath, mAlluxioPath,
        mDescendantType, mLoadByDirectory, mId);
  }
}
