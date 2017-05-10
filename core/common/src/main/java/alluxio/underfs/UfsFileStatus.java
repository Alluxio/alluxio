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

package alluxio.underfs;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information about a file in {@link UnderFileSystem}.
 */
@NotThreadSafe
public class UfsFileStatus extends UfsStatus {
  protected final long mContentLength;
  protected final long mLastModifiedTimeMs;

  /**
   * Creates new instance of {@link UfsFileStatus}.
   *
   * @param name relative path of file
   * @param contentLength in bytes
   * @param lastModifiedTimeMs UTC time
   * @param owner of the file
   * @param group of the file
   * @param mode of the file
   */
  public UfsFileStatus(String name, long contentLength, long lastModifiedTimeMs, String owner,
      String group, short mode) {
    super(name, false, owner, group, mode);
    mContentLength = contentLength;
    mLastModifiedTimeMs = lastModifiedTimeMs;
  }

  /**
   * Creates a new instance of {@link UfsFileStatus} as a copy.
   *
   * @param status file information to copy
   */
  public UfsFileStatus(UfsFileStatus status) {
    super(status);
    mContentLength = status.mContentLength;
    mLastModifiedTimeMs = status.mLastModifiedTimeMs;
  }

  @Override
  public UfsFileStatus copy() {
    return new UfsFileStatus(this);
  }

  /**
   * Get the content size in bytes.
   *
   * @return file size in bytes
   */
  public long getContentLength() {
    return mContentLength;
  }

  /**
   * Gets the UTC time of when the indicated path was modified recently in ms.
   *
   * @return modification time in milliseconds
   */
  public long getLastModifiedTime() {
    return mLastModifiedTimeMs;
  }
}
