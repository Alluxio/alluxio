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

import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information about a file in {@link UnderFileSystem}.
 */
@NotThreadSafe
public class UfsFileStatus extends UfsStatus {
  public static final String INVALID_CONTENT_HASH = "";
  public static final long UNKNOWN_BLOCK_SIZE = -1;

  protected final String mContentHash;
  protected final long mContentLength;
  protected final long mBlockSize;

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
   * @param xAttr extended attributes, if any
   * @param blockSize blocksize, -1 if unknown
   */
  public UfsFileStatus(String name, String contentHash, long contentLength, long lastModifiedTimeMs,
      String owner, String group, short mode, @Nullable Map<String, byte[]> xAttr, long blockSize) {
    super(name, false, owner, group, mode, lastModifiedTimeMs, xAttr);
    mContentHash = contentHash;
    mContentLength = contentLength;
    mBlockSize = blockSize;
  }

  /**
   * Creates new instance of {@link UfsFileStatus} without any extended attributes.
   *
   * @param name relative path of file
   * @param contentHash hash of the file contents
   * @param contentLength in bytes
   * @param lastModifiedTimeMs UTC time
   * @param owner of the file
   * @param group of the file
   * @param mode of the file
   * @param blockSize blocksize, -1 if unknown
   */
  public UfsFileStatus(String name, String contentHash, long contentLength, long lastModifiedTimeMs,
      String owner, String group, short mode, long blockSize) {
    super(name, false, owner, group, mode, lastModifiedTimeMs, /* xattrs */ null);
    mContentHash = contentHash;
    mContentLength = contentLength;
    mBlockSize = blockSize;
  }

  /**
   * Creates new instance of {@link UfsFileStatus}.
   *
   * @deprecated as of 2.1.0, use
   * {@link #UfsFileStatus(String, String, long, long, String, String, short, Map, long)}
   *
   * @param name relative path of file
   * @param contentHash hash of the file contents
   * @param contentLength in bytes
   * @param lastModifiedTimeMs UTC time
   * @param owner of the file
   * @param group of the file
   * @param mode of the file
   * @param xAttr extended attributes, if any
   */
  @Deprecated
  public UfsFileStatus(String name, String contentHash, long contentLength, long lastModifiedTimeMs,
      String owner, String group, short mode, @Nullable Map<String, byte[]> xAttr) {
    this(name, contentHash, contentLength, lastModifiedTimeMs, owner, group, mode, xAttr,
        UNKNOWN_BLOCK_SIZE);
  }

  /**
   * Creates a new instance of {@link UfsFileStatus} without any extended attributes.
   *
   * @deprecated as of 2.1.0, use
   * {@link #UfsFileStatus(String, String, long, long, String, String, short, long)}.
   *
   * @param name relative path of file
   * @param contentHash hash of the file contents
   * @param contentLength in bytes
   * @param lastModifiedTimeMs UTC time
   * @param owner of the file
   * @param group of the file
   * @param mode of the file
   */
  @Deprecated
  public UfsFileStatus(String name, String contentHash, long contentLength, long lastModifiedTimeMs,
      String owner, String group, short mode) {
    this(name, contentHash, contentLength, lastModifiedTimeMs, owner, group, mode, null,
        UNKNOWN_BLOCK_SIZE);
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
    mBlockSize = status.mBlockSize;
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

  /**
   * @return the block size in bytes, -1 if unknown
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("contentHash", mContentHash)
        .add("contentLength", mContentLength)
        .toString();
  }
}
