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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Target buffer backed by nio WritableByteChannel for zero-copy read from page store.
 */
public class WritableChannelTargetBuffer implements PageReadTargetBuffer {

  private final WritableByteChannel mChannel;
  private final long mCapacity;
  private long mOffset;

  /**
   * Constructor.
   * @param channel target writable channel
   * @param capacity write capacity of the writable chaanel
   */
  public WritableChannelTargetBuffer(WritableByteChannel channel, long capacity) {
    mChannel = channel;
    mCapacity = capacity;
  }

  @Override
  public byte[] byteArray() {
    throw new UnsupportedOperationException();
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
  public WritableByteChannel byteChannel() {
    return mChannel;
  }

  @Override
  public long remaining() {
    return mCapacity - mOffset;
  }

  @Override
  public void writeBytes(byte[] srcArray, int srcOffset, int length) throws IOException {
    ByteBuffer srcBuf = ByteBuffer.wrap(srcArray, srcOffset, length);
    while (srcBuf.hasRemaining()) {
      mChannel.write(srcBuf);
    }
    mOffset += length;
  }

  @Override
  public int readFromFile(RandomAccessFile file, int length) throws IOException {
    int bytesRead = (int) file.getChannel().transferTo(0, length, mChannel);
    mOffset += bytesRead;
    return bytesRead;
  }
}
