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

/**
 * This represents the overall metadata sync task information.
 */
class TaskInfo {
  private final AlluxioURI mBasePath;
  private final DescendantType mDescendantType;
  private final long mId;
  private final DirectoryLoadType mLoadByDirectory;

  TaskInfo(
      AlluxioURI basePath, DescendantType descendantType,
      DirectoryLoadType loadByDirectory, long id) {
    mBasePath = basePath;
    mDescendantType = descendantType;
    mLoadByDirectory = loadByDirectory;
    mId = id;
  }

  AlluxioURI getBasePath() {
    return mBasePath;
  }

  long getId() {
    return mId;
  }

  DirectoryLoadType getLoadByDirectory() {
    return mLoadByDirectory;
  }

  public DescendantType getDescendantType() {
    return mDescendantType;
  }

  @Override
  public String toString() {
    return String.format(
        "TaskInfo{Base path: %s, Descendant Type: %s, Id: %d}", mBasePath, mDescendantType, mId);
  }
}
