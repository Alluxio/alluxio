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

package tachyon.network.protocol.databuffer;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Tests for the {@link DataNettyBuffer} class.
 */
public class DataNettyBufferTest {
  private static final int LENGTH = 4;

  private ByteBuf mBuffer = null;

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up the buffer before a test runs.
   */
  @Before
  public final void before() {
    mBuffer = Unpooled.buffer(LENGTH);
  }

  /**
   * Releases the buffer a test ran.
   */
  @After
  public final void after() {
    releaseBuffer();
  }

  private void releaseBuffer() {
    if (mBuffer != null && mBuffer.refCnt() > 0) {
      mBuffer.release(mBuffer.refCnt());
    }
  }

  /**
   * Tests that an exception is thrown when two NIO buffers are used.
   */
  @Test
  public void singleNioBufferCheckFailedTest() {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("Number of nioBuffers of this bytebuf is 2 (1 expected).");
    releaseBuffer(); // not using the default ByteBuf given in Before()
    // creating a CompositeByteBuf with 2 NIO buffers
    mBuffer = Unpooled.compositeBuffer();
    ((CompositeByteBuf) mBuffer).addComponent(Unpooled.buffer(LENGTH));
    ((CompositeByteBuf) mBuffer).addComponent(Unpooled.buffer(LENGTH));
    new DataNettyBuffer(mBuffer, LENGTH);
  }

  /**
   * Tests that an exception is thrown when the reference count is two.
   */
  @Test
  public void refCountCheckFailedTest() {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("Reference count of this bytebuf is 2 (1 expected).");
    mBuffer.retain(); // increase reference count by 1
    new DataNettyBuffer(mBuffer, LENGTH);
  }

  /**
   * Tests that an exception is thrown when the unsupported {@link DataNettyBuffer#getNettyOutput()}
   * method is used.
   */
  @Test
  public void getNettyOutputNotSupportedTest() {
    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("DataNettyBuffer doesn't support getNettyOutput()");
    DataNettyBuffer data = new DataNettyBuffer(mBuffer, LENGTH);
    data.getNettyOutput();
  }

  /**
   * Tests the {@link DataNettyBuffer#getLength()} method.
   */
  @Test
  public void lengthTest() {
    DataNettyBuffer data = new DataNettyBuffer(mBuffer, LENGTH);
    Assert.assertEquals(LENGTH, data.getLength());
  }

  /**
   * Tests the {@link DataNettyBuffer#getReadOnlyByteBuffer()} method.
   */
  @Test
  public void readOnlyByteBufferTest() {
    DataNettyBuffer data = new DataNettyBuffer(mBuffer, LENGTH);
    ByteBuffer readOnlyBuffer = data.getReadOnlyByteBuffer();
    Assert.assertTrue(readOnlyBuffer.isReadOnly());
    Assert.assertEquals(mBuffer.nioBuffer(), readOnlyBuffer);
  }

  /**
   * Tests the {@link DataNettyBuffer#release()} method.
   */
  @Test
  public void releaseBufferTest() {
    DataNettyBuffer data = new DataNettyBuffer(mBuffer, LENGTH);
    mBuffer.release(); // this simulates a release performed by message channel
    data.release();
  }

  /**
   * Tests that an exception is thrown when the {@link DataNettyBuffer#release()} method fails with
   * a reference count of two netty buffers.
   */
  @Test
  public void releaseBufferFailTest() {
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage("Reference count of the netty buffer is 2 (1 expected).");
    DataNettyBuffer data = new DataNettyBuffer(mBuffer, LENGTH);
    data.release();
  }

  /**
   * Tests that an exception is thrown when the buffer was already released via the
   * {@link DataNettyBuffer#release()} method.
   */
  @Test
  public void bufferAlreadyReleasedTest() {
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage("Reference count of the netty buffer is 0 (1 expected).");
    DataNettyBuffer data = new DataNettyBuffer(mBuffer, LENGTH);
    mBuffer.release(); // this simulates a release performed by message channel
    mBuffer.release(); // this simulates an additional release
    data.release();
  }
}
