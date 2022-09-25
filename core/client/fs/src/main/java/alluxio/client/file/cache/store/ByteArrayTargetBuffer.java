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
  private int mOffset;

  /**
   * Constructor.
   * @param target
   * @param offset
   */
  public ByteArrayTargetBuffer(byte[] target, int offset) {
    mTarget = target;
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
  public long offset() {
    return mOffset;
  }

  @Override
  public long remaining() {
    return mTarget.length - mOffset;
  }

  @Override
  public void writeBytes(byte[] srcArray, int srcOffset, int length) {
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
