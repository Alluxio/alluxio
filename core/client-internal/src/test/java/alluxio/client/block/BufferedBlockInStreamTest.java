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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link BufferedBlockInStream} class.
 */
public class BufferedBlockInStreamTest {
  private static final long BLOCK_LENGTH = 100L;

  private TestBufferedBlockInStream mTestStream;

  /**
   * Sets up the stream before a test runs.
   */
  @Before
  public void before() {
    mTestStream = new TestBufferedBlockInStream(1L, 0, BLOCK_LENGTH);
  }

  /**
   * Verifies the byte by byte read is equal to an increasing byte array, where the written data is
   * an increasing byte array.
   *
   * @throws Exception when reading from the stream fails
   */
  @Test
  public void singleByteReadTest() throws Exception {
    for (int i = 0; i < BLOCK_LENGTH; i++) {
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
    mTestStream.seek(BLOCK_LENGTH);
    Assert.assertEquals(-1, mTestStream.read());
  }

  /**
   * Tests that {@link BufferedBlockInStream#read(byte[], int, int)} works for bulk reads.
   *
   * @throws Exception when reading from the stream fails
   */
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
