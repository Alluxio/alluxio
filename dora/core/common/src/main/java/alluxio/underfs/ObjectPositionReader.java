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

abstract public class ObjectPositionReader implements PositionReader {
  /** Path of the file when it is uploaded to object storage. */
  protected final String mPath;

  /** Length of the file. */
  protected final long mFileLength;

  /** Name of the bucket the object resides in. */
  protected final String mBucketName;

  /**
   * @param bucketName the bucket name
   * @param path path of the file when it is uploaded to object storage.
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
    String errorMessage = String
        .format("Failed to get object: %s bucket: %s", mPath, mBucketName);
    // request to object service client and return getObjectContent().
    try(InputStream in = getRequestInputStream(position, buffer, bytesToRead, errorMessage)){
      // Range check approach: set range (inclusive start, inclusive end)
      // start: should be < file length, error out otherwise
      //        e.g. error out when start == 0 && fileLength == 0
      //        start < 0, read all
      // end: if start > end, read all
      //      if start <= end < file length, read from start to end
      //      if end >= file length, read from start to file length - 1
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
    catch (IOException e) {
      throw new IOException(errorMessage, e);
    }
  }

  /**
   * @param position position of the file to start reading data
   * @param buffer target byte buffer
   * @param bytesToRead bytes to read
   * @return bytes read, or -1 none of data is read
   */
  abstract protected InputStream getRequestInputStream(
      long position, ReadTargetBuffer buffer,
      int bytesToRead, String errorMessage) throws IOException;

}
