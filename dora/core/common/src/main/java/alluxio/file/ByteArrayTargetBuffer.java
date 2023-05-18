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

package alluxio.file;

import alluxio.annotation.SuppressFBWarnings;
import alluxio.util.io.ChannelAdapters;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Target buffer backed by bytes array for zero-copy read from page store.
 */
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification = "The target byte array is exposed as we expect.")
public class ByteArrayTargetBuffer implements ReadTargetBuffer {
  private final byte[] mTarget;
  private final int mLimit;
  private int mOffset;

  /**
   * Constructor. The initial offset will be 0, and the limit will be the length of the array.
   *
   * @param target the target buffer
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
    return ByteBuffer.wrap(mTarget, mOffset, remaining());
  }

  @Override
  public int offset() {
    return mOffset;
  }

  @Override
  public void offset(int newOffset) {
    Preconditions.checkArgument(newOffset >= 0 && newOffset <= mLimit, "offset");
    mOffset = newOffset;
  }

  @Override
  public int remaining() {
    return mLimit - mOffset;
  }

  @Override
  public void writeBytes(byte[] srcArray, int srcOffset, int length) {
    Preconditions.checkArgument(length <= remaining(),
        "buffer overflow: bytes remaining: %s, bytes to copy: %s", remaining(), length);
    System.arraycopy(srcArray, srcOffset, mTarget, mOffset, length);
    mOffset += length;
  }

  @Override
  public void writeBytes(ByteBuf buf) {
    int bytesToRead = buf.readableBytes();
    Preconditions.checkArgument(bytesToRead <= remaining(),
        "buffer overflow: bytes remaining: %s, bytes to copy: %s", remaining(), bytesToRead);
    buf.readBytes(mTarget, mOffset, bytesToRead);
    mOffset += bytesToRead;
  }

  @Override
  public int readFromFile(RandomAccessFile file, int length) throws IOException {
    Preconditions.checkArgument(length <= remaining(),
        "buffer overflow: bytes remaining: %s, bytes to copy: %s", remaining(), length);
    int bytesRead = file.read(mTarget, mOffset, length);
    if (bytesRead != -1) {
      mOffset += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public int readFromInputStream(InputStream is, int length) throws IOException {
    Preconditions.checkArgument(length <= remaining(),
        "buffer overflow: bytes remaining: %s, bytes to copy: %s", remaining(), length);
    int bytesRead = is.read(mTarget, mOffset, length);
    if (bytesRead != -1) {
      mOffset += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public WritableByteChannel byteChannel() {
    return ChannelAdapters.intoByteArray(mTarget, mOffset, remaining());
  }
}
