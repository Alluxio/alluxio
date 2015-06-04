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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RPCBlockRequestTest {
  private static final long BLOCK_ID = 11;
  private static final long OFFSET = 22;
  private static final long LENGTH = 33;

  private ByteBuf mBuffer = null;

  private void assertValid(long blockId, long offset, long length, RPCBlockRequest req) {
    Assert.assertEquals(RPCMessage.Type.RPC_BLOCK_REQUEST, req.getType());
    Assert.assertEquals(blockId, req.getBlockId());
    Assert.assertEquals(offset, req.getOffset());
    Assert.assertEquals(length, req.getLength());
  }

  private void assertValid(RPCBlockRequest req) {
    try {
      req.validate();
    } catch (Exception e) {
      Assert.fail("request should be valid.");
    }
  }

  private void assertInvalid(RPCBlockRequest req) {
    try {
      req.validate();
      Assert.fail("request should be invalid.");
    } catch (Exception e) {
      return;
    }
  }

  @Before
  public final void before() {
    mBuffer = Unpooled.buffer();
  }

  @Test
  public void encodedLengthTest() {
    RPCBlockRequest req = new RPCBlockRequest(BLOCK_ID, OFFSET, LENGTH);
    int encodedLength = req.getEncodedLength();
    req.encode(mBuffer);
    Assert.assertEquals(encodedLength, mBuffer.readableBytes());
  }

  @Test
  public void encodeDecodeTest() {
    RPCBlockRequest req = new RPCBlockRequest(BLOCK_ID, OFFSET, LENGTH);
    req.encode(mBuffer);
    RPCBlockRequest req2 = RPCBlockRequest.decode(mBuffer);
    assertValid(BLOCK_ID, OFFSET, LENGTH, req);
    assertValid(BLOCK_ID, OFFSET, LENGTH, req2);
  }

  @Test
  public void validateTest() {
    RPCBlockRequest req = new RPCBlockRequest(BLOCK_ID, OFFSET, LENGTH);
    assertValid(req);
  }

  @Test
  public void validLengthTest() {
    RPCBlockRequest req = new RPCBlockRequest(BLOCK_ID, OFFSET, -1);
    assertValid(req);
    req = new RPCBlockRequest(BLOCK_ID, OFFSET, 0);
    assertValid(req);
  }

  @Test
  public void negativeOffsetTest() {
    RPCBlockRequest req = new RPCBlockRequest(BLOCK_ID, -1, LENGTH);
    assertInvalid(req);
  }

  @Test
  public void invalidLengthTest() {
    RPCBlockRequest req = new RPCBlockRequest(BLOCK_ID, OFFSET, -100);
    assertInvalid(req);
  }
}
