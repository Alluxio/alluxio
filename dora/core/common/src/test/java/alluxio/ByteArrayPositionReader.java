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

package alluxio;

import alluxio.file.ReadTargetBuffer;

import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * Test position reader that reads data from a byte array.
 */
public class ByteArrayPositionReader implements PositionReader {
  private final byte[] mData;

  /**
   * Creates a new reader from the byte array.
   *
   * @param data data
   */
  public ByteArrayPositionReader(byte[] data) {
    mData = data;
  }

  @Override
  public int readInternal(long position, ReadTargetBuffer buffer, int length) throws IOException {
    Preconditions.checkArgument(length >= 0, "negative length: %s", length);
    if (position > mData.length) {
      throw new ArrayIndexOutOfBoundsException("Index: " + position + ", length: " + mData.length);
    }
    if (length == 0) {
      return 0;
    }
    if (position == mData.length) {
      return -1;
    }
    buffer.writeBytes(mData, (int) position, length);
    return length;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}
