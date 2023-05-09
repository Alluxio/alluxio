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

  /**
   * Constructs an instance of {@link SyncProcessResult}.
   *
   * @param taskInfo the task info
   * @param baseLoadPath the base load path
   * @param loaded the path sequence
   * @param isTruncated whether the result is truncated or not
   * @param rootPathIsFile whether the root path is a file or not
   */
  public SyncProcessResult(
      TaskInfo taskInfo, AlluxioURI baseLoadPath,
      @Nullable PathSequence loaded, boolean isTruncated,
      boolean rootPathIsFile) {
    mRootPathIsFile = rootPathIsFile;
    mBaseLoadPath = baseLoadPath;
    mTaskInfo = taskInfo;
    mLoaded = loaded;
    mIsTruncated = isTruncated;
  }

  /**
   * @return true if the root path is a file, false otherwise
   */
  public boolean rootPathIsFile() {
    return mRootPathIsFile;
  }

  /**
   * @return the base load path
   */
  public AlluxioURI getBaseLoadPath() {
    return mBaseLoadPath;
  }

  /**
   * @return true if the result is truncated, false otherwise
   */
  public boolean isTruncated() {
    return mIsTruncated;
  }

  /**
   * @return Optional containing the loaded path sequence
   */
  public Optional<PathSequence> getLoaded() {
    return Optional.ofNullable(mLoaded);
  }

  /**
   * @return the task info
   */
  public TaskInfo getTaskInfo() {
    return mTaskInfo;
  }
}
