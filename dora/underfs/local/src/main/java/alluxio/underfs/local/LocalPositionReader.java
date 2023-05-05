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

package alluxio.underfs.local;

import alluxio.PositionReader;
import alluxio.file.ReadTargetBuffer;

import java.io.IOException;
import java.io.RandomAccessFile;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link PositionReader} that reads from local UFS.
 */
@ThreadSafe
public class LocalPositionReader implements PositionReader {
  private final String mPath;
  private final long mFileLength;

  /**
   * @param path the local file path
   * @param fileLength the file length
   */
  public LocalPositionReader(String path, long fileLength) {
    mPath = path;
    mFileLength = fileLength;
  }

  @Override
  public int readInternal(long position, ReadTargetBuffer buffer, int length)
      throws IOException {
    if (position >= mFileLength) { // at end of file
      return -1;
    }
    long bytesToRead = Math.min(mFileLength - position, length);
    try (RandomAccessFile file = new RandomAccessFile(mPath, "r")) {
      file.seek(position);
      return buffer.readFromFile(file, (int) bytesToRead);
    }
  }
}
