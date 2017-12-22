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

package alluxio.worker.block;

import alluxio.underfs.SeekableUnderFileInputStream;

import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * A seekable input stream that can be cached.
 */
class CachedSeekableInputStream extends SeekableUnderFileInputStream {
  /** A unique resource id annotated for resource tracking. */
  private final Long mResourceId;
  /** The file path of the input stream. */
  private final String mFilePath;
  /** The file Id. */
  private final long mFileId;

  /**
   * Creates a new {@link CachedSeekableInputStream}.
   *
   * @param inputStream the input stream from the under storage
   * @param resourceId the resource id
   * @param fileId the file id
   * @param filePath the file path
   */
  CachedSeekableInputStream(SeekableUnderFileInputStream inputStream, long resourceId, long fileId,
      String filePath) {
    super(inputStream);
    Preconditions.checkArgument(resourceId >= 0, "resource id should be positive");
    mResourceId = resourceId;
    mFilePath = filePath;
    mFileId = fileId;
  }

  /**
   * @return the resource id
   */
  long getResourceId() {
    return mResourceId;
  }

  /**
   * @return the under file path
   */
  String getFilePath() {
    return mFilePath;
  }

  /**
   * @return the file id
   */
  Long getFileId() {
    return mFileId;
  }

  @Override
  public void seek(long pos) throws IOException {
    ((SeekableUnderFileInputStream) in).seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    return ((SeekableUnderFileInputStream) in).getPos();
  }
}
