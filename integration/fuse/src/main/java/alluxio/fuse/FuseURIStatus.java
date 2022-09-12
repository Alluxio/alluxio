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

package alluxio.fuse;

import alluxio.client.file.URIStatus;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents the metadata about a file or directory used by Alluxio POSIX API
 */
@ThreadSafe
public class FuseURIStatus {
  // TODO(lu) change to optional?
  private final String mName;
  private final long mLength;
  private final boolean mCompleted;
  private final boolean mFolder;
  private final long mLastModificationTimeMs;
  private final long mLastAccessTimeMs;
  private final String mOwner;
  private final String mGroup;
  private final int mMode;
  
  public FuseURIStatus(URIStatus uriStatus) {
    mName = uriStatus.getName();
    mLength = uriStatus.getLength();
    mCompleted = uriStatus.isCompleted();
    mFolder = uriStatus.isFolder();
    mLastModificationTimeMs = uriStatus.getLastModificationTimeMs();
    mLastAccessTimeMs = uriStatus.getLastAccessTimeMs();
    mOwner = uriStatus.getOwner();
    mGroup = uriStatus.getGroup();
    mMode = uriStatus.getMode();
  }

  public FuseURIStatus(String name, long length, boolean completed, long lastModificationTimeMs,
    long lastAccessTime, String owner, String group, int mode) {
    mName = name;
    mLength = length;
    mCompleted = completed;
    mFolder = isFolder();
    mLastModificationTimeMs = lastModificationTimeMs;
    mLastAccessTimeMs = lastAccessTime;
    mOwner = owner;
    mGroup = group;
    mMode = mode;
  }

  /**
   * @return the file name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the file length
   */
  public long getLength() {
    return mLength;
  }


  /**
   * @return the file last modification time (in milliseconds)
   */
  public long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }

  /**
   * @return the file last access time (in milliseconds)
   */
  public long getLastAccessTimeMs() {
    return mLastAccessTimeMs;
  }


  /**
   * @return the file owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return the file owner group
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the file mode bits
   */
  public int getMode() {
    return mMode;
  }

  /**
   * @return whether the file is completed
   */
  public boolean isCompleted() {
    return mCompleted;
  }

  /**
   * @return whether the file is a folder
   */
  public boolean isFolder() {
    return mFolder;
  }
}
