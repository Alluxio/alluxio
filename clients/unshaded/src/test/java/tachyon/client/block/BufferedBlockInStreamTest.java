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
import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.util.io.BufferUtils;

public class BufferedBlockInStreamTest {
  private class TestBufferedBlockInStream extends BufferedBlockInStream {
    private final byte[] mData;

    private int mBytesRead;

    public TestBufferedBlockInStream(long blockId, long blockSize, InetSocketAddress location) {
      super(blockId, blockSize, location);
      mData = BufferUtils.getIncreasingByteArray((int) blockSize);
      mBytesRead = 0;
    }

    public int getBytesRead() {
      return mBytesRead;
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
    protected void incrementBytesReadMetric(int bytes) {
      mBytesRead += bytes;
    }
  }

  private static final long BLOCK_LENGTH = 100L;

  private TestBufferedBlockInStream mTestStream;

  @Before
  public void before() {
    mTestStream = new TestBufferedBlockInStream(1L, BLOCK_LENGTH, null);
  }

  @Test
  public void singleByteReadTest() throws Exception {
    // Verify byte by byte read is equal to increasing byte array
    for (int i = 0; i < BLOCK_LENGTH; i ++) {
      Assert.assertEquals(i, mTestStream.read());
    }
  }

  @Test
  public void skipTest() throws Exception {
    // Skip forward
    Assert.assertEquals(10, mTestStream.skip(10));
    Assert.assertEquals(10, mTestStream.read());

    // Skip 0
    Assert.assertEquals(0, mTestStream.skip(0));
    Assert.assertEquals(11, mTestStream.read());
  }

  @Test
  public void seekTest() throws Exception {
    // Seek forward
    mTestStream.seek(10);
    Assert.assertEquals(10, mTestStream.read());

    // Seek backward
    mTestStream.seek(2);
    Assert.assertEquals(2, mTestStream.read());

    // Seek to end
    mTestStream.seek(BLOCK_LENGTH);
    Assert.assertEquals(-1, mTestStream.read());
  }

  @Test
  public void bulkReadTest() throws Exception {
    int size = (int) BLOCK_LENGTH / 10;
    byte[] readBytes = new byte[size];

    // Read first 10 bytes
    Assert.assertEquals(size, mTestStream.read(readBytes));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(0, size, readBytes));

    // Read next 10 bytes
    Assert.assertEquals(size, mTestStream.read(readBytes));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(size, size, readBytes));

    // Read with offset and length
    Assert.assertEquals(1, mTestStream.read(readBytes, size - 1, 1));
    Assert.assertEquals(size * 2, readBytes[size - 1]);
  }
}
