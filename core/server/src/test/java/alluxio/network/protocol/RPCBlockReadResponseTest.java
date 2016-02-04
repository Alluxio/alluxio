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

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import alluxio.network.protocol.databuffer.DataByteBuffer;

/**
 * Unit tests for {@link RPCBlockReadResponse}.
 */
public class RPCBlockReadResponseTest {
  private static final long BLOCK_ID = 1;
  private static final long OFFSET = 2;
  // The RPCMessageEncoder sends the payload separately from the message, so these unit tests only
  // test the message encoding part. Therefore, the 'length' should be 0.
  private static final long LENGTH = 0;
  private static final long LOCK_ID = 4444;
  private static final long SESSION_ID = 5555;

  private static final RPCResponse.Status STATUS = RPCResponse.Status.SUCCESS;

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private ByteBuf mBuffer = null;

  private void assertValid(long blockId, long offset, long length, RPCResponse.Status status,
      RPCBlockReadResponse resp) {
    Assert.assertEquals(RPCMessage.Type.RPC_BLOCK_READ_RESPONSE, resp.getType());
    Assert.assertEquals(blockId, resp.getBlockId());
    Assert.assertEquals(offset, resp.getOffset());
    Assert.assertEquals(length, resp.getLength());
    Assert.assertEquals(status, resp.getStatus());
  }

  private void assertValid(RPCBlockReadResponse resp) {
    try {
      resp.validate();
    } catch (Exception e) {
      Assert.fail("response should be valid.");
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
   * Tests the {@link RPCBlockReadResponse#getEncodedLength()} method.
   */
  @Test
  public void encodedLengthTest() {
    RPCBlockReadResponse resp = new RPCBlockReadResponse(BLOCK_ID, OFFSET, LENGTH, null, STATUS);
    int encodedLength = resp.getEncodedLength();
    resp.encode(mBuffer);
    Assert.assertEquals(encodedLength, mBuffer.readableBytes());
  }

  /**
   * Tests the {@link RPCBlockReadResponse#encode(ByteBuf)} and
   * {@link RPCBlockReadResponse#decode(ByteBuf)} methods.
   */
  @Test
  public void encodeDecodeTest() {
    RPCBlockReadResponse resp = new RPCBlockReadResponse(BLOCK_ID, OFFSET, LENGTH, null, STATUS);
    resp.encode(mBuffer);
    RPCBlockReadResponse resp2 = RPCBlockReadResponse.decode(mBuffer);
    assertValid(BLOCK_ID, OFFSET, LENGTH, STATUS, resp);
    assertValid(BLOCK_ID, OFFSET, LENGTH, STATUS, resp2);
  }

  /**
   * Tests the {@link RPCBlockReadResponse#validate()} method.
   */
  @Test
  public void validateTest() {
    RPCBlockReadResponse resp = new RPCBlockReadResponse(BLOCK_ID, OFFSET, LENGTH, null, STATUS);
    assertValid(resp);
  }

  /**
   * Tests the {@link RPCBlockReadResponse#getPayloadDataBuffer()} method.
   */
  @Test
  public void getPayloadDataBufferTest() {
    int length = 10;
    DataByteBuffer payload = new DataByteBuffer(ByteBuffer.allocate(length), length);
    RPCBlockReadResponse resp = new RPCBlockReadResponse(BLOCK_ID, OFFSET, LENGTH, payload, STATUS);
    assertValid(resp);
    Assert.assertEquals(payload, resp.getPayloadDataBuffer());
  }

  /**
   * Tests the
   * {@link RPCBlockReadResponse#createErrorResponse(RPCBlockReadRequest, RPCResponse.Status)}
   * method.
   */
  @Test
  public void createErrorResponseTest() {
    RPCBlockReadRequest req = new RPCBlockReadRequest(BLOCK_ID, OFFSET, LENGTH, LOCK_ID,
        SESSION_ID);

    for (RPCResponse.Status status : RPCResponse.Status.values()) {
      if (status == RPCResponse.Status.SUCCESS) {
        // cannot create an error response with a SUCCESS status.
        mThrown.expect(IllegalArgumentException.class);
        RPCBlockReadResponse.createErrorResponse(req, status);
      } else {
        RPCBlockReadResponse resp = RPCBlockReadResponse.createErrorResponse(req, status);
        assertValid(BLOCK_ID, OFFSET, 0, status, resp);
      }
    }
  }
}
