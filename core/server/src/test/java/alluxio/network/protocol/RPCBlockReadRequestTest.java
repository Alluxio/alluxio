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

package alluxio.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link RPCBlockReadRequest}.
 */
public class RPCBlockReadRequestTest {
  private static final long BLOCK_ID = 11;
  private static final long OFFSET = 22;
  private static final long LENGTH = 33;
  private static final long LOCK_ID = 44;
  private static final long SESSION_ID = 55;

  private ByteBuf mBuffer = null;

  private void assertValid(long blockId, long offset, long length, long lockId, long sessionId,
      RPCBlockReadRequest req) {
    Assert.assertEquals(RPCMessage.Type.RPC_BLOCK_READ_REQUEST, req.getType());
    Assert.assertEquals(blockId, req.getBlockId());
    Assert.assertEquals(offset, req.getOffset());
    Assert.assertEquals(length, req.getLength());
    Assert.assertEquals(lockId, req.getLockId());
    Assert.assertEquals(sessionId, req.getSessionId());
  }

  private void assertValid(RPCBlockReadRequest req) {
    try {
      req.validate();
    } catch (Exception e) {
      Assert.fail("request should be valid.");
    }
  }

  private void assertInvalid(RPCBlockReadRequest req) {
    try {
      req.validate();
      Assert.fail("request should be invalid.");
    } catch (Exception e) {
      return;
    }
  }

  /**
   * Sets up the buffer before a test runs.
   */
  @Before
  public final void before() {
    mBuffer = Unpooled.buffer();
  }

  /**
   * Tests the {@link RPCBlockReadRequest#getEncodedLength()} method.
   */
  @Test
  public void encodedLengthTest() {
    RPCBlockReadRequest req = new RPCBlockReadRequest(BLOCK_ID, OFFSET, LENGTH, LOCK_ID,
        SESSION_ID);
    int encodedLength = req.getEncodedLength();
    req.encode(mBuffer);
    Assert.assertEquals(encodedLength, mBuffer.readableBytes());
  }

  /**
   * Tests the {@link RPCBlockReadRequest#encode(ByteBuf)} and
   * {@link RPCBlockReadRequest#decode(ByteBuf)} methods.
   */
  @Test
  public void encodeDecodeTest() {
    RPCBlockReadRequest req = new RPCBlockReadRequest(BLOCK_ID, OFFSET, LENGTH, LOCK_ID,
        SESSION_ID);
    req.encode(mBuffer);
    RPCBlockReadRequest req2 = RPCBlockReadRequest.decode(mBuffer);
    assertValid(BLOCK_ID, OFFSET, LENGTH, LOCK_ID, SESSION_ID, req);
    assertValid(BLOCK_ID, OFFSET, LENGTH, LOCK_ID, SESSION_ID, req2);
  }

  /**
   * Tests the {@link RPCBlockReadRequest#validate()} method.
   */
  @Test
  public void validateTest() {
    RPCBlockReadRequest req = new RPCBlockReadRequest(BLOCK_ID, OFFSET, LENGTH, LOCK_ID,
        SESSION_ID);
    assertValid(req);
  }

  /**
   * Tests the {@link RPCBlockReadRequest#RPCBlockReadRequest(long, long, long, long, long)}
   * constructor with a valid length.
   */
  @Test
  public void validLengthTest() {
    RPCBlockReadRequest req = new RPCBlockReadRequest(BLOCK_ID, OFFSET, -1, LOCK_ID,
        SESSION_ID);
    assertValid(req);
    req = new RPCBlockReadRequest(BLOCK_ID, OFFSET, 0, LOCK_ID, SESSION_ID);
    assertValid(req);
  }

  /**
   * Tests the {@link RPCBlockReadRequest#RPCBlockReadRequest(long, long, long, long, long)}
   * constructor with a negative offset.
   */
  @Test
  public void negativeOffsetTest() {
    RPCBlockReadRequest req = new RPCBlockReadRequest(BLOCK_ID, -1, LENGTH, LOCK_ID,
        SESSION_ID);
    assertInvalid(req);
  }

  /**
   * Tests the {@link RPCBlockReadRequest#RPCBlockReadRequest(long, long, long, long, long)}
   * constructor with an invalid length.
   */
  @Test
  public void invalidLengthTest() {
    RPCBlockReadRequest req = new RPCBlockReadRequest(BLOCK_ID, OFFSET, -100, LOCK_ID,
        SESSION_ID);
    assertInvalid(req);
  }
}
