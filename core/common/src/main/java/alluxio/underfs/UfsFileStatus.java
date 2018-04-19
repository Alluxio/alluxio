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
  public static final String INVALID_CONTENT_HASH = "";

  protected final String mContentHash;
  protected final long mContentLength;

  /**
   * Creates new instance of {@link UfsFileStatus}.
   *
   * @param name relative path of file
   * @param contentHash hash of the file contents
   * @param contentLength in bytes
   * @param lastModifiedTimeMs UTC time
   * @param owner of the file
   * @param group of the file
   * @param mode of the file
   */
  public UfsFileStatus(String name, String contentHash, long contentLength, long lastModifiedTimeMs,
      String owner, String group, short mode) {
    super(name, false, owner, group, mode, lastModifiedTimeMs);
    mContentHash = contentHash;
    mContentLength = contentLength;
  }

  /**
   * Creates a new instance of {@link UfsFileStatus} as a copy.
   *
   * @param status file information to copy
   */
  public UfsFileStatus(UfsFileStatus status) {
    super(status);
    mContentHash = status.mContentHash;
    mContentLength = status.mContentLength;
  }

  @Override
  public UfsFileStatus copy() {
    return new UfsFileStatus(this);
  }

  /**
   * @return the hash of the file contents
   */
  public String getContentHash() {
    return mContentHash;
  }

  /**
   * Get the content size in bytes.
   *
   * @return file size in bytes
   */
  public long getContentLength() {
    return mContentLength;
  }
}
