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
    mTestStream = new TestBufferedBlockOutStream(1L, BLOCK_LENGTH, BlockStoreContext.get());
  }

  /**
   * Tests for the {@link BufferedBlockOutStream#remaining()} method.
   */
  @Test
  public void remaining() {
    mTestStream.setWrittenBytes(BLOCK_LENGTH);
    Assert.assertEquals(0L, mTestStream.remaining());

    mTestStream.setWrittenBytes(40L);
    Assert.assertEquals(BLOCK_LENGTH - 40L, mTestStream.remaining());

    mTestStream.setWrittenBytes(0L);
    Assert.assertEquals(BLOCK_LENGTH, mTestStream.remaining());
  }

  /**
   * Tests writing an increasing byte array one byte at a time.
   */
  @Test
  public void singleByteWrite() throws Exception {
    for (int i = 0; i < BLOCK_LENGTH; i++) {
      mTestStream.write(INCREASING_BYTES[i]);
      Assert.assertEquals(i + 1, mTestStream.getWrittenBytes());
    }
    Assert.assertArrayEquals(INCREASING_BYTES,
        Arrays.copyOfRange(mTestStream.getBuffer().array(), 0, (int) BLOCK_LENGTH));
  }

  /**
   * Tests writing an increasing byte array.
   */
  @Test
  public void byteArrayWrite() throws Exception {
    mTestStream.write(INCREASING_BYTES);
    Assert.assertEquals(INCREASING_BYTES.length, mTestStream.getWrittenBytes());
    Assert.assertArrayEquals(INCREASING_BYTES,
        Arrays.copyOfRange(mTestStream.getBuffer().array(), 0, (int) BLOCK_LENGTH));
  }

  /**
   * Tests writing the middle half of an increasing byte array and test writing more than half the
   * buffer limit. This causes an unbuffered write and flush.
   */
  @Test
  public void byteArrayAtOffset() throws Exception {
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
   */
  @Test
  public void writeToClosed() throws Exception {
    mTestStream.close();
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage(PreconditionMessage.ERR_CLOSED_BLOCK_OUT_STREAM.toString());
    mTestStream.write(0);
  }

  /**
   * Tests that writing past a block throws an exception.
   */
  @Test
  public void writePastBlock() throws Exception {
    mTestStream.setWrittenBytes(BLOCK_LENGTH);
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage(PreconditionMessage.ERR_END_OF_BLOCK.toString());
    mTestStream.write(0);
  }

  /**
   * Tests that flushing twice works.
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
