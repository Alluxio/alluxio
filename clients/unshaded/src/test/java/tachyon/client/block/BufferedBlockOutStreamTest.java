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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.exception.PreconditionMessage;
import tachyon.util.io.BufferUtils;

public class BufferedBlockOutStreamTest {
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private static final long BLOCK_LENGTH = 100L;
  private static final byte[] INCREASING_BYTES =
      BufferUtils.getIncreasingByteArray((int) BLOCK_LENGTH);

  private TestBufferedBlockOutStream mTestStream;

  @Before
  public void before() {
    mTestStream = new TestBufferedBlockOutStream(1L, BLOCK_LENGTH);
  }

  @Test
  public void remainingTest() {
    mTestStream.setWrittenBytes(BLOCK_LENGTH);
    Assert.assertEquals(0L, mTestStream.remaining());

    mTestStream.setWrittenBytes(40L);
    Assert.assertEquals(BLOCK_LENGTH - 40L, mTestStream.remaining());

    mTestStream.setWrittenBytes(0L);
    Assert.assertEquals(BLOCK_LENGTH, mTestStream.remaining());
  }

  @Test
  public void singleByteWriteTest() throws Exception {
    // Test writing an increasing byte array one byte at a time
    for (int i = 0; i < BLOCK_LENGTH; i ++) {
      mTestStream.write(INCREASING_BYTES[i]);
      Assert.assertEquals(i + 1, mTestStream.getBytesWritten());
    }
    Assert.assertArrayEquals(INCREASING_BYTES,
        Arrays.copyOfRange(mTestStream.getBuffer().array(), 0, (int) BLOCK_LENGTH));
  }

  @Test
  public void byteArrayWriteTest() throws Exception {
    // Test writing an increasing byte array
    mTestStream.write(INCREASING_BYTES);
    Assert.assertEquals(INCREASING_BYTES.length, mTestStream.getBytesWritten());
    Assert.assertArrayEquals(INCREASING_BYTES,
        Arrays.copyOfRange(mTestStream.getBuffer().array(), 0, (int) BLOCK_LENGTH));
  }

  @Test
  public void byteArrayAtOffsetTest() throws Exception {
    // Test writing the middle half of an increasing byte array
    mTestStream.write(INCREASING_BYTES, 25, 50);
    Assert.assertEquals(50, mTestStream.getBytesWritten());
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(25, 50),
        Arrays.copyOfRange(mTestStream.getBuffer().array(), 0, 50));

    // Test writing more than half the buffer limit. This causes an unbuffered write and flush
    Assert.assertFalse(mTestStream.mHasFlushed);
    int bytesToWrite = mTestStream.getBuffer().limit() / 2 + 1;
    mTestStream.write(INCREASING_BYTES, 30, bytesToWrite);
    Assert.assertTrue(mTestStream.mHasFlushed);
    Assert.assertSame(INCREASING_BYTES, mTestStream.mLastBufferedWriteArray);
    Assert.assertEquals(30, mTestStream.mLastBufferedWriteOffset);
    Assert.assertEquals(bytesToWrite, mTestStream.mLastBufferedWriteLen);
    Assert.assertEquals(50 + mTestStream.mLastBufferedWriteLen, mTestStream.getBytesWritten());
  }

  @Test
  public void writeToClosed() throws Exception {
    mTestStream.close();
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage(PreconditionMessage.ERR_CLOSED_BLOCK_OUT_STREAM);
    mTestStream.write(0);
  }

  @Test
  public void writePastBlock() throws Exception {
    mTestStream.setWrittenBytes(BLOCK_LENGTH);
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage(PreconditionMessage.ERR_END_OF_BLOCK);
    mTestStream.write(0);
  }
}
