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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;

import tachyon.util.io.BufferUtils;

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
  public void nettyOutputTest() {
    DataByteBuffer data = new DataByteBuffer(mBuffer, LENGTH);
    Object output = data.getNettyOutput();
    Assert.assertTrue(output instanceof ByteBuf || output instanceof FileRegion);
  }

  /**
   * Tests the {@link DataByteBuffer#getLength()} method.
   */
  @Test
  public void lengthTest() {
    DataByteBuffer data = new DataByteBuffer(mBuffer, LENGTH);
    Assert.assertEquals(LENGTH, data.getLength());
  }

  /**
   * Tests the {@link DataByteBuffer#getReadOnlyByteBuffer()} method.
   */
  @Test
  public void readOnlyByteBufferTest() {
    DataByteBuffer data = new DataByteBuffer(mBuffer, LENGTH);
    ByteBuffer readOnlyBuffer = data.getReadOnlyByteBuffer();
    Assert.assertTrue(readOnlyBuffer.isReadOnly());
    Assert.assertEquals(mBuffer, readOnlyBuffer);
  }
}
