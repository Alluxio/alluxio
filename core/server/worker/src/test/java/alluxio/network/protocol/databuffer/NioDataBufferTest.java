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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import alluxio.util.io.BufferUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Tests for the {@link NioDataBuffer} class.
 */
public class NioDataBufferTest {
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
   * Tests the {@link NioDataBuffer#getNettyOutput()} method.
   */
  @Test
  public void nettyOutput() {
    NioDataBuffer data = new NioDataBuffer(mBuffer, LENGTH);
    Object output = data.getNettyOutput();
    assertTrue(output instanceof ByteBuf || output instanceof FileRegion);
  }

  /**
   * Tests the {@link NioDataBuffer#getLength()} method.
   */
  @Test
  public void length() {
    NioDataBuffer data = new NioDataBuffer(mBuffer, LENGTH);
    assertEquals(LENGTH, data.getLength());
  }

  /**
   * Tests the {@link NioDataBuffer#getReadOnlyByteBuffer()} method.
   */
  @Test
  public void readOnlyByteBuffer() {
    NioDataBuffer data = new NioDataBuffer(mBuffer, LENGTH);
    ByteBuffer readOnlyBuffer = data.getReadOnlyByteBuffer();
    assertTrue(readOnlyBuffer.isReadOnly());
    assertEquals(mBuffer, readOnlyBuffer);
  }
}
