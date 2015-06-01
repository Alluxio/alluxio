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

package tachyon.worker.netty.protocol.buffer;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;

public class DataByteBufferTest {
  public static final int LENGTH = 5;

  public ByteBuffer mBuffer = null;

  @Before
  public final void before() {
    mBuffer = ByteBuffer.allocate(LENGTH);
  }

  @Test
  public void nettyOutputTest() {
    DataByteBuffer data = new DataByteBuffer(mBuffer, LENGTH);
    Object output = data.getNettyOutput();
    Assert.assertTrue(output instanceof ByteBuf || output instanceof FileRegion);
  }

  @Test
  public void lengthTest() {
    DataByteBuffer data = new DataByteBuffer(mBuffer, LENGTH);
    Assert.assertEquals(LENGTH, data.getLength());
  }
}
