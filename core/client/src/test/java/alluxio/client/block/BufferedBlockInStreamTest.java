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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link BufferedBlockInStream} class.
 */
public class BufferedBlockInStreamTest {
  private TestBufferedBlockInStream mTestStream;
  private long mBlockSize;
  private long mBufferSize;

  /**
   * Sets up the stream before a test runs.
   */
  @Before
  public void before() {
    mBufferSize =
        Configuration.createDefaultConf().getBytes(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES);
    mBlockSize = mBufferSize * 10;
    mTestStream = new TestBufferedBlockInStream(1L, 0, mBlockSize);
  }

  /**
   * Verifies the byte by byte read is equal to an increasing byte array, where the written data is
   * an increasing byte array.
   *
   * @throws Exception when reading from the stream fails
   */
  @Test
  public void singleByteReadTest() throws Exception {
    for (int i = 0; i < mBlockSize; i++) {
      Assert.assertEquals(i, mTestStream.read());
    }
  }

  /**
   * Tests for the {@link BufferedBlockInStream#skip(long)} method.
   *
   * @throws Exception when an operation on the stream fails
   */
  @Test
  public void skipTest() throws Exception {
    // Skip forward
    Assert.assertEquals(10, mTestStream.skip(10));
    Assert.assertEquals(10, mTestStream.read());

    // Skip 0
    Assert.assertEquals(0, mTestStream.skip(0));
    Assert.assertEquals(11, mTestStream.read());
  }

  /**
   * Tests for the {@link BufferedBlockInStream#seek(long)} method.
   *
   * @throws Exception when an operation on the stream fails
   */
  @Test
  public void seekTest() throws Exception {
    // Seek forward
    mTestStream.seek(10);
    Assert.assertEquals(10, mTestStream.read());

    // Seek backward
    mTestStream.seek(2);
    Assert.assertEquals(2, mTestStream.read());

    // Seek to end
    mTestStream.seek(mBlockSize);
    Assert.assertEquals(-1, mTestStream.read());
  }

  /**
   * Tests that {@link BufferedBlockInStream#read(byte[], int, int)} works for bulk reads.
   *
   * @throws Exception when reading from the stream fails
   */
  @Test
  public void bulkReadTest() throws Exception {
    int size = (int) mBlockSize / 10;
    byte[] readBytes = new byte[size];

    // Read first 10th bytes
    Assert.assertEquals(size, mTestStream.read(readBytes));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(0, size, readBytes));

    // Read next 10th bytes
    Assert.assertEquals(size, mTestStream.read(readBytes));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(size, size, readBytes));

    // Read with offset and length
    Assert.assertEquals(1, mTestStream.read(readBytes, size - 1, 1));
    Assert.assertEquals(size * 2, readBytes[size - 1]);
  }

  /**
   * Tests that {@link BufferedBlockInStream#read(byte[], int, int)} buffering logic.
   *
   * @throws Exception when reading from the stream fails
   */
  @Test
  public void bufferReadTest() throws Exception {
    int position = 0;
    int size = (int) mBufferSize / 2;
    byte[] readBytes = new byte[size];
    long shouldRemain = mBufferSize - size;

    // Read half buffer, should be from buffer
    Assert.assertEquals(size, mTestStream.read(readBytes));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(position, size, readBytes));
    Assert.assertEquals(shouldRemain, mTestStream.mBuffer.remaining());

    position += size;
    shouldRemain -= size;

    // Read next half buffer, should be from buffer
    Assert.assertEquals(size, mTestStream.read(readBytes));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(position, size, readBytes));
    Assert.assertEquals(shouldRemain, mTestStream.mBuffer.remaining());

    position += size;
    size = (int) mBufferSize;
    readBytes = new byte[size];

    // Read buffer size bytes, should not be from buffer
    Assert.assertEquals(size, mTestStream.read(readBytes));
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(position, size, readBytes));
    Assert.assertEquals(shouldRemain, mTestStream.mBuffer.remaining());
  }
}
