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

package alluxio.fuse.file;

/**
 * The file status of ongoing fuse stream.
 */
public class FileStatus {
  private long mFileLength;

  /**
   * Constructs a new {@link FileStatus}.
   *
   * @param fileLength the initial file length
   */
  public FileStatus(long fileLength) {
    mFileLength = fileLength;
  }

  /**
   * @return the length of the file
   */
  public long getFileLength() {
    return mFileLength;
  }

  /**
   * Sets the length of the file.
   *
   * @param fileLength the file length
   */
  public void setFileLength(long fileLength) {
    mFileLength = fileLength;
  }
}
