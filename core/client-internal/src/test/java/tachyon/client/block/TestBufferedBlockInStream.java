/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.block;

import java.io.IOException;

import tachyon.util.io.BufferUtils;

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
