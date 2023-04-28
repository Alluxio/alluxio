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

package alluxio.hadoop;

import alluxio.PositionReader;
import alluxio.file.ByteArrayTargetBuffer;
import alluxio.file.ReadTargetBuffer;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * A wrapper class to translate Hadoop FileSystem FSDataInputStream to Alluxio FileSystem
 * PositionReader.
 */
public class AlluxioHdfsPositionReader implements PositionReader {
  private final FSDataInputStream mInput;
  private final long mFileSize;
  private volatile boolean mClosed;

  /**
   * @param input Hadoop FileSystem FSDataInputStream
   * @param fileSize the file size
   */
  public AlluxioHdfsPositionReader(FSDataInputStream input, long fileSize) {
    mInput = Preconditions.checkNotNull(input, "null");
    mFileSize = fileSize;
  }

  @Override
  public int readInternal(long position, ReadTargetBuffer buffer, int length) throws IOException {
    Preconditions.checkArgument(!mClosed, "position reader is closed");
    if (position >= mFileSize) { // at end of file
      return -1;
    }
    boolean targetIsByteArray = buffer instanceof ByteArrayTargetBuffer;
    int lengthToRead = (int) Math.min(length, mFileSize - position);
    byte[] byteArray = targetIsByteArray ? buffer.byteArray() : new byte[lengthToRead];
    int arrayPosition = targetIsByteArray ? buffer.offset() : 0;
    // TODO(lu) read from HDFS more efficiently by leveraging ByteBufferPositionedReadable
    // TODO(lu) read logics put in ReadTargetBuffer
    mInput.readFully(position, byteArray, arrayPosition, lengthToRead);
    if (targetIsByteArray) {
      buffer.offset(arrayPosition + lengthToRead);
    } else {
      buffer.writeBytes(byteArray, 0, lengthToRead);
    }
    return lengthToRead;
  }

  @Override
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mInput.close();
  }
}
