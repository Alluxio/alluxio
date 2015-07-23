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

public class DataNettyBufferTest {
  private static final int LENGTH = 4;

  private ByteBuf mBuffer = null;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public final void before() {
    mBuffer = Unpooled.buffer(LENGTH);
  }

  @After
  public final void after() {
    releaseBuffer();
  }

  private void releaseBuffer() {
    if (mBuffer != null && mBuffer.refCnt() > 0) {
      mBuffer.release(mBuffer.refCnt());
    }
  }

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

  @Test
  public void refCountCheckFailedTest() {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage("Reference count of this bytebuf is 2 (1 expected).");
    mBuffer.retain(); // increase reference count by 1
    new DataNettyBuffer(mBuffer, LENGTH);
  }

  @Test
  public void getNettyOutputNotSupportedTest() {
    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("DataNettyBuffer doesn't support getNettyOutput()");
    DataNettyBuffer data = new DataNettyBuffer(mBuffer, LENGTH);
    data.getNettyOutput();
  }

  @Test
  public void lengthTest() {
    DataNettyBuffer data = new DataNettyBuffer(mBuffer, LENGTH);
    Assert.assertEquals(LENGTH, data.getLength());
  }

  @Test
  public void readOnlyByteBufferTest() {
    DataNettyBuffer data = new DataNettyBuffer(mBuffer, LENGTH);
    ByteBuffer readOnlyBuffer = data.getReadOnlyByteBuffer();
    Assert.assertTrue(readOnlyBuffer.isReadOnly());
    Assert.assertEquals(mBuffer.nioBuffer(), readOnlyBuffer);
  }

  @Test
  public void releaseBufferTest() {
    DataNettyBuffer data = new DataNettyBuffer(mBuffer, LENGTH);
    mBuffer.release(); // this simulates a release performed by message channel
    data.release();
  }

  @Test
  public void releaseBufferFailTest() {
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage("Reference count of the netty buffer is 2 (1 expected).");
    DataNettyBuffer data = new DataNettyBuffer(mBuffer, LENGTH);
    data.release();
  }

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
