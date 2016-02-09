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

package alluxio.client.block;

import alluxio.exception.PreconditionMessage;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

/**
 * Tests for the {@link BufferedBlockOutStream} class.
 */
public class BufferedBlockOutStreamTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private static final long BLOCK_LENGTH = 100L;
  private static final byte[] INCREASING_BYTES =
      BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH);

  private TestBufferedBlockOutStream mTestStream;

  /**
   * Sets up the stream before a test runs.
   */
  @Before
  public void before() {
    mTestStream = new TestBufferedBlockOutStream(1L, BLOCK_LENGTH);
  }

  /**
   * Tests for the {@link BufferedBlockOutStream#remaining()} method.
   */
  @Test
  public void remainingTest() {
    mTestStream.setWrittenBytes(BLOCK_LENGTH);
    Assert.assertEquals(0L, mTestStream.remaining());

    mTestStream.setWrittenBytes(40L);
    Assert.assertEquals(BLOCK_LENGTH - 40L, mTestStream.remaining());

    mTestStream.setWrittenBytes(0L);
    Assert.assertEquals(BLOCK_LENGTH, mTestStream.remaining());
  }

  /**
   * Tests writing an increasing byte array one byte at a time.
   *
   * @throws Exception when an operation on the stream fails
   */
  @Test
  public void singleByteWriteTest() throws Exception {
    for (int i = 0; i < BLOCK_LENGTH; i ++) {
      mTestStream.write(INCREASING_BYTES[i]);
      Assert.assertEquals(i + 1, mTestStream.getWrittenBytes());
    }
    Assert.assertArrayEquals(INCREASING_BYTES,
        Arrays.copyOfRange(mTestStream.getBuffer().array(), 0, (int) BLOCK_LENGTH));
  }

  /**
   * Tests writing an increasing byte array.
   *
   * @throws Exception when an operation on the stream fails
   */
  @Test
  public void byteArrayWriteTest() throws Exception {
    mTestStream.write(INCREASING_BYTES);
    Assert.assertEquals(INCREASING_BYTES.length, mTestStream.getWrittenBytes());
    Assert.assertArrayEquals(INCREASING_BYTES,
        Arrays.copyOfRange(mTestStream.getBuffer().array(), 0, (int) BLOCK_LENGTH));
  }

  /**
   * Tests writing the middle half of an increasing byte array and test writing more than half the
   * buffer limit. This causes an unbuffered write and flush.
   *
   * @throws Exception when an operation on the stream fails
   */
  @Test
  public void byteArrayAtOffsetTest() throws Exception {
    mTestStream.write(INCREASING_BYTES, 25, 50);
    Assert.assertEquals(50, mTestStream.getWrittenBytes());
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(25, 50),
        Arrays.copyOfRange(mTestStream.getBuffer().array(), 0, 50));

    Assert.assertFalse(mTestStream.mHasFlushed);
    int bytesToWrite = mTestStream.getBuffer().limit() / 2 + 1;
    mTestStream.write(INCREASING_BYTES, 30, bytesToWrite);
    Assert.assertTrue(mTestStream.mHasFlushed);
    Assert.assertSame(INCREASING_BYTES, mTestStream.mLastBufferedWriteArray);
    Assert.assertEquals(30, mTestStream.mLastBufferedWriteOffset);
    Assert.assertEquals(bytesToWrite, mTestStream.mLastBufferedWriteLen);
    Assert.assertEquals(50 + mTestStream.mLastBufferedWriteLen, mTestStream.getWrittenBytes());
  }

  /**
   * Tests that writing to a closed stream throws an exception.
   *
   * @throws Exception when an operation on the stream fails
   */
  @Test
  public void writeToClosed() throws Exception {
    mTestStream.close();
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage(PreconditionMessage.ERR_CLOSED_BLOCK_OUT_STREAM);
    mTestStream.write(0);
  }

  /**
   * Tests that writing past a block throws an exception.
   *
   * @throws Exception when an operation on the stream fails
   */
  @Test
  public void writePastBlock() throws Exception {
    mTestStream.setWrittenBytes(BLOCK_LENGTH);
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage(PreconditionMessage.ERR_END_OF_BLOCK);
    mTestStream.write(0);
  }

  /**
   * Tests that flushing twice works.
   *
   * @throws Exception when an operation on the stream fails
   */
  @Test
  public void doubleFlush() throws Exception {
    mTestStream.write(INCREASING_BYTES, 1, 10);
    Assert.assertEquals(0, mTestStream.getFlushedBytes());
    mTestStream.flush();
    Assert.assertEquals(10, mTestStream.getFlushedBytes());
    mTestStream.flush();
    Assert.assertEquals(10, mTestStream.getFlushedBytes());
  }
}
