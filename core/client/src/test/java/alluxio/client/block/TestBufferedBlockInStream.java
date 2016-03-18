/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.util.io.BufferUtils;

import java.io.IOException;

/**
 * Test class for mocking {@link BufferedBlockInStream}. The stream will read in increasing bytes
 * from `start` to `start + blockSize`.
 */
public class TestBufferedBlockInStream extends BufferedBlockInStream {
  private final byte[] mData;

  /**
   * Constructs a new {@link TestBufferedBlockInStream} to be used in tests.
   *
   * @param blockId the id of the block
   * @param start the position to start to read
   * @param blockSize the size of the block in bytes
   */
  public TestBufferedBlockInStream(long blockId, int start, long blockSize) {
    super(blockId, blockSize);
    mData = BufferUtils.getIncreasingByteArray(start, (int) blockSize);
  }

  @Override
  protected void bufferedRead(int len) throws IOException {
    mBuffer.clear();
    mBuffer.put(mData, (int) getPosition(), len);
    mBuffer.flip();
  }

  @Override
  protected int directRead(byte[] b, int off, int len) throws IOException {
    System.arraycopy(mData, (int) getPosition(), b, off, len);
    return len;
  }

  @Override
  protected void incrementBytesReadMetric(int bytes) {}
}
