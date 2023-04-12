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

package alluxio.client.file.cache.store;

import alluxio.annotation.SuppressFBWarnings;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Target buffer backed by bytes array for zero-copy read from page store.
 */
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification = "The target byte array is exposed as we expect.")
public class ByteArrayTargetBuffer implements PageReadTargetBuffer {
  private final byte[] mTarget;
  private final int mLimit;
  private int mOffset;

  /**
   * Constructor.
   * This sets the offset to 0 and the length to the length of the byte array.
   *
   * @param target
   */
  public ByteArrayTargetBuffer(byte[] target) {
    this(target, 0, target.length);
  }

  /**
   * Constructor.
   * @param target the target buffer
   * @param offset the starting index of the byte array where data is written to
   * @param length the maximum length of data that will be written to the array
   */
  public ByteArrayTargetBuffer(byte[] target, int offset, int length) {
    Preconditions.checkArgument(offset >= 0 && offset <= target.length, "offset");
    Preconditions.checkArgument(length >= 0 && length <= target.length - offset, "length");
    mTarget = target;
    mLimit = offset + length;
    mOffset = offset;
  }

  @Override
  public byte[] byteArray() {
    return mTarget;
  }

  @Override
  public ByteBuffer byteBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int offset() {
    return mOffset;
  }

  @Override
  public int remaining() {
    return mLimit - mOffset;
  }

  @Override
  public void writeBytes(byte[] srcArray, int srcOffset, int length) {
    if (length > remaining()) {
      throw new IndexOutOfBoundsException(String.format(
          "length: %d, remaining in buffer: %d", length, remaining()));
    }
    System.arraycopy(srcArray, srcOffset, mTarget, mOffset, length);
    mOffset += length;
  }

  @Override
  public int readFromFile(RandomAccessFile file, int length) throws IOException {
    int bytesRead = file.read(mTarget, mOffset, length);
    if (bytesRead != -1) {
      mOffset += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public WritableByteChannel byteChannel() {
    throw new UnsupportedOperationException();
  }
}
