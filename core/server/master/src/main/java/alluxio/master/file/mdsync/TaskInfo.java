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
import alluxio.conf.path.TrieNode;
import alluxio.file.options.DescendantType;
import alluxio.file.options.DirectoryLoadType;

import java.util.stream.Stream;
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
  private final MetadataSyncHandler mMetadataSyncHandler;
  private final TaskStats mStats;

  private final TrieNode<AlluxioURI> mPathsToUpdateDirectChildrenLoaded = new TrieNode<>();

  TaskInfo(
      MetadataSyncHandler metadataSyncHandler,
      AlluxioURI ufsPath, // basePath should be without the header/bucket, e.g. no s3://
      AlluxioURI alluxioPath,
      @Nullable String startAfter,
      DescendantType descendantType,
      long syncInterval,
      DirectoryLoadType loadByDirectory,
      long id) {
    mBasePath = ufsPath;
    mAlluxioPath = alluxioPath;
    mSyncInterval = syncInterval;
    mDescendantType = descendantType;
    mLoadByDirectory = loadByDirectory;
    mId = id;
    mStartAfter = startAfter;
    mMetadataSyncHandler = metadataSyncHandler;
    mStats = new TaskStats();
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
  public MetadataSyncHandler getMdSync() {
    return mMetadataSyncHandler;
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
  DirectoryLoadType getLoadByDirectory() {
    return mLoadByDirectory;
  }

  /**
   * @return the descendant type
   */
  public DescendantType getDescendantType() {
    return mDescendantType;
  }

  @Override
  public String toString() {
    return String.format(
        "TaskInfo{UFS path: %s, AlluxioPath: %s, Descendant Type: %s,"
            + " Directory Load Type: %s, Id: %d}", mBasePath, mAlluxioPath,
        mDescendantType, mLoadByDirectory, mId);
  }

  /**
   * @return the paths need to update direct children loaded
   */
  synchronized Stream<AlluxioURI> getPathsToUpdateDirectChildrenLoaded() {
    return mPathsToUpdateDirectChildrenLoaded.getLeafChildren("/").map(TrieNode::getValue);
  }

  /**
   * Add path to set direct children loaded. This call must be synchronized
   * as it will be called by different threads while processing tasks.
   * @param uri to update direct children loaded
   */
  synchronized void addPathToUpdateDirectChildrenLoaded(AlluxioURI uri) {
    mPathsToUpdateDirectChildrenLoaded.insert(uri.getPath()).setValue(uri);
  }
}
