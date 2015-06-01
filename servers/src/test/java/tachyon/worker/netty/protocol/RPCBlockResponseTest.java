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

package tachyon.worker.netty.protocol;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RPCBlockResponseTest {
  public static final long BLOCK_ID = 1;
  public static final long OFFSET = 2;
  // The RPCMessageEncoder sends the payload separately from the message, so these unit tests only
  // test the message encoding part. Therefore, the 'length' should be 0.
  public static final long LENGTH = 0;

  private ByteBuf mBuffer = null;

  private void assertValid(long blockId, long offset, long length, RPCBlockResponse req) {
    Assert.assertEquals(blockId, req.getBlockId());
    Assert.assertEquals(offset, req.getOffset());
    Assert.assertEquals(length, req.getLength());
  }

  private void assertValid(RPCBlockResponse req) {
    try {
      req.validate();
    } catch (Exception e) {
      Assert.fail("request should be valid.");
    }
  }

  @Before
  public final void before() {
    mBuffer = Unpooled.buffer();
  }

  @Test
  public void encodedLengthTest() {
    RPCBlockResponse req = new RPCBlockResponse(BLOCK_ID, OFFSET, LENGTH, null);
    int encodedLength = req.getEncodedLength();
    req.encode(mBuffer);
    Assert.assertEquals(encodedLength, mBuffer.readableBytes());
  }

  @Test
  public void encodeDecodeTest() {
    RPCBlockResponse req = new RPCBlockResponse(BLOCK_ID, OFFSET, LENGTH, null);
    req.encode(mBuffer);
    RPCBlockResponse req2 = RPCBlockResponse.decode(mBuffer);
    assertValid(BLOCK_ID, OFFSET, LENGTH, req);
    assertValid(BLOCK_ID, OFFSET, LENGTH, req2);
  }

  @Test
  public void validateTest() {
    RPCBlockResponse req = new RPCBlockResponse(BLOCK_ID, OFFSET, LENGTH, null);
    assertValid(req);
  }
}
