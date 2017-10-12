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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link RPCFileWriteResponse}.
 */
public class RPCFileWriteResponseTest {
  private static final long TEMP_UFS_FILE_ID = 11;
  private static final long OFFSET = 22;
  private static final long LENGTH = 33;

  private static final RPCResponse.Status STATUS = RPCResponse.Status.SUCCESS;

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private ByteBuf mBuffer = null;

  private void assertValid(long tempUfsFileId, long offset, long length, RPCResponse.Status status,
      RPCFileWriteResponse resp) {
    assertEquals(RPCMessage.Type.RPC_FILE_WRITE_RESPONSE, resp.getType());
    assertEquals(tempUfsFileId, resp.getTempUfsFileId());
    assertEquals(offset, resp.getOffset());
    assertEquals(length, resp.getLength());
    assertEquals(status, resp.getStatus());
  }

  private void assertValid(RPCFileWriteResponse resp) {
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
   * Tests the {@link RPCFileWriteResponse#getEncodedLength()} method.
   */
  @Test
  public void encodedLength() {
    RPCFileWriteResponse resp = new RPCFileWriteResponse(TEMP_UFS_FILE_ID, OFFSET, LENGTH, STATUS);
    int encodedLength = resp.getEncodedLength();
    resp.encode(mBuffer);
    assertEquals(encodedLength, mBuffer.readableBytes());
  }

  /**
   * Tests the {@link RPCFileWriteResponse#encode(ByteBuf)} and
   * {@link RPCFileWriteResponse#decode(ByteBuf)} methods.
   */
  @Test
  public void encodeDecode() {
    RPCFileWriteResponse resp = new RPCFileWriteResponse(TEMP_UFS_FILE_ID, OFFSET, LENGTH, STATUS);
    resp.encode(mBuffer);
    RPCFileWriteResponse resp2 = RPCFileWriteResponse.decode(mBuffer);
    assertValid(TEMP_UFS_FILE_ID, OFFSET, LENGTH, STATUS, resp);
    assertValid(TEMP_UFS_FILE_ID, OFFSET, LENGTH, STATUS, resp2);
  }

  /**
   * Tests the {@link RPCBlockReadResponse#validate()} method.
   */
  @Test
  public void validate() {
    RPCFileWriteResponse resp = new RPCFileWriteResponse(TEMP_UFS_FILE_ID, OFFSET, LENGTH, STATUS);
    assertValid(resp);
  }

  /**
   * Tests the
   * {@link RPCBlockReadResponse#createErrorResponse(RPCBlockReadRequest, RPCResponse.Status)}
   * method.
   */
  @Test
  public void createErrorResponse() {
    RPCFileWriteRequest req = new RPCFileWriteRequest(TEMP_UFS_FILE_ID, OFFSET, LENGTH, null);

    for (RPCResponse.Status status : RPCResponse.Status.values()) {
      if (status == RPCResponse.Status.SUCCESS) {
        // cannot create an error response with a SUCCESS status.
        mThrown.expect(IllegalArgumentException.class);
        RPCFileWriteResponse.createErrorResponse(req, status);
      } else {
        RPCFileWriteResponse resp = RPCFileWriteResponse.createErrorResponse(req, status);
        assertValid(TEMP_UFS_FILE_ID, OFFSET, 0, status, resp);
      }
    }
  }
}
