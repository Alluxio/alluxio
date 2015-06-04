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

package tachyon.network.protocol;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import tachyon.network.protocol.RPCBlockResponse;
import tachyon.network.protocol.RPCMessage;
import tachyon.network.protocol.buffer.DataByteBuffer;

public class RPCBlockResponseTest {
  private static final long BLOCK_ID = 1;
  private static final long OFFSET = 2;
  // The RPCMessageEncoder sends the payload separately from the message, so these unit tests only
  // test the message encoding part. Therefore, the 'length' should be 0.
  private static final long LENGTH = 0;

  private ByteBuf mBuffer = null;

  private void assertValid(long blockId, long offset, long length, RPCBlockResponse resp) {
    Assert.assertEquals(RPCMessage.Type.RPC_BLOCK_RESPONSE, resp.getType());
    Assert.assertEquals(blockId, resp.getBlockId());
    Assert.assertEquals(offset, resp.getOffset());
    Assert.assertEquals(length, resp.getLength());
  }

  private void assertValid(RPCBlockResponse resp) {
    try {
      resp.validate();
    } catch (Exception e) {
      Assert.fail("response should be valid.");
    }
  }

  @Before
  public final void before() {
    mBuffer = Unpooled.buffer();
  }

  @Test
  public void encodedLengthTest() {
    RPCBlockResponse resp = new RPCBlockResponse(BLOCK_ID, OFFSET, LENGTH, null);
    int encodedLength = resp.getEncodedLength();
    resp.encode(mBuffer);
    Assert.assertEquals(encodedLength, mBuffer.readableBytes());
  }

  @Test
  public void encodeDecodeTest() {
    RPCBlockResponse resp = new RPCBlockResponse(BLOCK_ID, OFFSET, LENGTH, null);
    resp.encode(mBuffer);
    RPCBlockResponse resp2 = RPCBlockResponse.decode(mBuffer);
    assertValid(BLOCK_ID, OFFSET, LENGTH, resp);
    assertValid(BLOCK_ID, OFFSET, LENGTH, resp2);
  }

  @Test
  public void validateTest() {
    RPCBlockResponse resp = new RPCBlockResponse(BLOCK_ID, OFFSET, LENGTH, null);
    assertValid(resp);
  }

  @Test
  public void getPayloadDataBufferTest() {
    int length = 10;
    DataByteBuffer payload = new DataByteBuffer(ByteBuffer.allocate(length), length);
    RPCBlockResponse resp = new RPCBlockResponse(BLOCK_ID, OFFSET, LENGTH, payload);
    assertValid(resp);
    Assert.assertEquals(payload, resp.getPayloadDataBuffer());
  }
}
