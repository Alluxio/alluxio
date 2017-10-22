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

package alluxio.network.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import alluxio.network.protocol.databuffer.DataByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;

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
    assertEquals(RPCMessage.Type.RPC_BLOCK_READ_RESPONSE, resp.getType());
    assertEquals(blockId, resp.getBlockId());
    assertEquals(offset, resp.getOffset());
    assertEquals(length, resp.getLength());
    assertEquals(status, resp.getStatus());
  }

  private void assertValid(RPCBlockReadResponse resp) {
    try {
      resp.validate();
    } catch (Exception e) {
      fail("response should be valid.");
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
  public void encodedLength() {
    RPCBlockReadResponse resp = new RPCBlockReadResponse(BLOCK_ID, OFFSET, LENGTH, null, STATUS);
    int encodedLength = resp.getEncodedLength();
    resp.encode(mBuffer);
    assertEquals(encodedLength, mBuffer.readableBytes());
  }

  /**
   * Tests the {@link RPCBlockReadResponse#encode(ByteBuf)} and
   * {@link RPCBlockReadResponse#decode(ByteBuf)} methods.
   */
  @Test
  public void encodeDecode() {
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
  public void validate() {
    RPCBlockReadResponse resp = new RPCBlockReadResponse(BLOCK_ID, OFFSET, LENGTH, null, STATUS);
    assertValid(resp);
  }

  /**
   * Tests the {@link RPCBlockReadResponse#getPayloadDataBuffer()} method.
   */
  @Test
  public void getPayloadDataBuffer() {
    int length = 10;
    DataByteBuffer payload = new DataByteBuffer(ByteBuffer.allocate(length), length);
    RPCBlockReadResponse resp = new RPCBlockReadResponse(BLOCK_ID, OFFSET, LENGTH, payload, STATUS);
    assertValid(resp);
    assertEquals(payload, resp.getPayloadDataBuffer());
  }

  /**
   * Tests the
   * {@link RPCBlockReadResponse#createErrorResponse(RPCBlockReadRequest, RPCResponse.Status)}
   * method.
   */
  @Test
  public void createErrorResponse() {
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
