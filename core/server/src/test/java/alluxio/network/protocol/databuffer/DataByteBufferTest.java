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

package alluxio.network.protocol.databuffer;

import alluxio.util.io.BufferUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Tests for the {@link DataByteBuffer} class.
 */
public class DataByteBufferTest {
  private static final int LENGTH = 5;

  private ByteBuffer mBuffer = null;

  /**
   * Sets up a new {@link ByteBuffer} before a test runs.
   */
  @Before
  public final void before() {
    mBuffer = BufferUtils.getIncreasingByteBuffer(LENGTH);
  }

  /**
   * Tests the {@link DataByteBuffer#getNettyOutput()} method.
   */
  @Test
  public void nettyOutput() {
    DataByteBuffer data = new DataByteBuffer(mBuffer, LENGTH);
    Object output = data.getNettyOutput();
    Assert.assertTrue(output instanceof ByteBuf || output instanceof FileRegion);
  }

  /**
   * Tests the {@link DataByteBuffer#getLength()} method.
   */
  @Test
  public void length() {
    DataByteBuffer data = new DataByteBuffer(mBuffer, LENGTH);
    Assert.assertEquals(LENGTH, data.getLength());
  }

  /**
   * Tests the {@link DataByteBuffer#getReadOnlyByteBuffer()} method.
   */
  @Test
  public void readOnlyByteBuffer() {
    DataByteBuffer data = new DataByteBuffer(mBuffer, LENGTH);
    ByteBuffer readOnlyBuffer = data.getReadOnlyByteBuffer();
    Assert.assertTrue(readOnlyBuffer.isReadOnly());
    Assert.assertEquals(mBuffer, readOnlyBuffer);
  }
}
