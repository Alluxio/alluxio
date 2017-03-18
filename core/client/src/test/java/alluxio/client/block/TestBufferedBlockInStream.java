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

package alluxio.client.block;

import java.io.IOException;

/**
 * Mock of {@link BufferedBlockInStream} to creates a BlockInStream on a single block. The
 * data of this stream will be read from the give byte array.
 */
public class TestBufferedBlockInStream extends BufferedBlockInStream {
  private final byte[] mData;

  /**
   * Constructs a new {@link TestBufferedBlockInStream} to be used in tests.
   *
   * @param blockId the id of the block
   * @param data data to read
   */
  public TestBufferedBlockInStream(long blockId, byte[] data) {
    super(blockId, data.length);
    mData = data;
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

  @Override
  boolean isLocal() {
    return true;
  }
}
