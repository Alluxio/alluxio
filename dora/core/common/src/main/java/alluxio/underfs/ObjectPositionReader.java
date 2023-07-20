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

import alluxio.PositionReader;
import alluxio.file.ReadTargetBuffer;

import java.io.IOException;
import java.io.InputStream;

/**
 * A stream for reading data using position reader.
 */

public abstract class ObjectPositionReader implements PositionReader {
  /** Path of the file when it is uploaded to object storage. */
  protected final String mPath;

  /** Length of the file. */
  protected final long mFileLength;

  /** Name of the bucket the object resides in. */
  protected final String mBucketName;

  /**
   * @param bucketName the bucket name
   * @param path path of the file when it is uploaded to object storage
   * @param fileLength the file length
   */
  public ObjectPositionReader(String bucketName, String path, long fileLength) {
    mBucketName = bucketName;
    mPath = path;
    mFileLength = fileLength;
  }

  /**
   * @param position position of the file to start reading data
   * @param buffer target byte buffer
   * @param length bytes to read
   * @return bytes read, or -1 none of data is read
   */
  @Override
  public int readInternal(long position, ReadTargetBuffer buffer, int length)
      throws IOException {
    if (position >= mFileLength) { // at end of file
      return -1;
    }
    int bytesToRead = (int) Math.min(mFileLength - position, length);
    try (InputStream in = openObjectInputStream(position, bytesToRead)) {
      int totalRead = 0;
      int currentRead = 0;
      while (totalRead < bytesToRead) {
        currentRead = buffer.readFromInputStream(in, bytesToRead - totalRead);
        if (currentRead < 0) {
          break;
        }
        totalRead += currentRead;
      }
      return totalRead == 0 ? currentRead : totalRead;
    }
  }

  /**
   * @param position position of the file to start reading data
   * @param bytesToRead bytes to read
   * @return input stream of Object Storage's API
   */
  protected abstract InputStream openObjectInputStream(
      long position, int bytesToRead) throws IOException;
}
