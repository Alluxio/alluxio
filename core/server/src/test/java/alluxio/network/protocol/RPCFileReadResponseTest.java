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

import alluxio.network.protocol.databuffer.DataByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;

/**
 * Unit tests for {@link RPCFileReadResponse}.
 */
public class RPCFileReadResponseTest {
  private static final long TEMP_UFS_FILE_ID = 11;
  private static final long OFFSET = 22;
  // The RPCMessageEncoder sends the payload separately from the message, so these unit tests only
  // test the message encoding part. Therefore, the 'length' should be 0.
  private static final long LENGTH = 0;

  private static final RPCResponse.Status STATUS = RPCResponse.Status.SUCCESS;

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private ByteBuf mBuffer = null;

  private void assertValid(long tempUfsFileId, long offset, long length, RPCResponse.Status status,
      RPCFileReadResponse resp) {
    Assert.assertEquals(RPCMessage.Type.RPC_FILE_READ_RESPONSE, resp.getType());
    Assert.assertEquals(tempUfsFileId, resp.getTempUfsFileId());
    Assert.assertEquals(offset, resp.getOffset());
    Assert.assertEquals(length, resp.getLength());
    Assert.assertEquals(status, resp.getStatus());
  }

  private void assertValid(RPCFileReadResponse resp) {
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
  public void encodedLength() {
    RPCFileReadResponse resp =
        new RPCFileReadResponse(TEMP_UFS_FILE_ID, OFFSET, LENGTH, null, STATUS);
    int encodedLength = resp.getEncodedLength();
    resp.encode(mBuffer);
    Assert.assertEquals(encodedLength, mBuffer.readableBytes());
  }

  /**
   * Tests the {@link RPCFileReadResponse#encode(ByteBuf)} and
   * {@link RPCFileReadResponse#decode(ByteBuf)} methods.
   */
  @Test
  public void encodeDecode() {
    RPCFileReadResponse resp =
        new RPCFileReadResponse(TEMP_UFS_FILE_ID, OFFSET, LENGTH, null, STATUS);
    resp.encode(mBuffer);
    RPCFileReadResponse resp2 = RPCFileReadResponse.decode(mBuffer);
    assertValid(TEMP_UFS_FILE_ID, OFFSET, LENGTH, STATUS, resp);
    assertValid(TEMP_UFS_FILE_ID, OFFSET, LENGTH, STATUS, resp2);
  }

  /**
   * Tests the {@link RPCBlockReadResponse#validate()} method.
   */
  @Test
  public void validate() {
    RPCFileReadResponse resp =
        new RPCFileReadResponse(TEMP_UFS_FILE_ID, OFFSET, LENGTH, null, STATUS);
    assertValid(resp);
  }

  /**
   * Tests the {@link RPCBlockReadResponse#getPayloadDataBuffer()} method.
   */
  @Test
  public void getPayloadDataBuffer() {
    int length = 10;
    DataByteBuffer payload = new DataByteBuffer(ByteBuffer.allocate(length), length);
    RPCFileReadResponse resp =
        new RPCFileReadResponse(TEMP_UFS_FILE_ID, OFFSET, LENGTH, payload, STATUS);
    assertValid(resp);
    Assert.assertEquals(payload, resp.getPayloadDataBuffer());
  }

  /**
   * Tests the
   * {@link RPCBlockReadResponse#createErrorResponse(RPCBlockReadRequest, RPCResponse.Status)}
   * method.
   */
  @Test
  public void createErrorResponse() {
    RPCFileReadRequest req = new RPCFileReadRequest(TEMP_UFS_FILE_ID, OFFSET, LENGTH);

    for (RPCResponse.Status status : RPCResponse.Status.values()) {
      if (status == RPCResponse.Status.SUCCESS) {
        // cannot create an error response with a SUCCESS status.
        mThrown.expect(IllegalArgumentException.class);
        RPCFileReadResponse.createErrorResponse(req, status);
      } else {
        RPCFileReadResponse resp = RPCFileReadResponse.createErrorResponse(req, status);
        assertValid(TEMP_UFS_FILE_ID, OFFSET, 0, status, resp);
      }
    }
  }
}
