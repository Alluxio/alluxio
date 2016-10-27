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

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Unit tests for {@link RPCFileWriteRequest}.
 */
public class RPCFileWriteRequestTest {
  private static final long TEMP_UFS_FILE_ID = 11;
  private static final long OFFSET = 22;
  private static final long LENGTH = 0;

  private ByteBuf mBuffer = null;

  private void assertValid(long tempUfsFileId, long offset, long length, RPCFileWriteRequest req) {
    Assert.assertEquals(RPCMessage.Type.RPC_FILE_WRITE_REQUEST, req.getType());
    Assert.assertEquals(tempUfsFileId, req.getTempUfsFileId());
    Assert.assertEquals(offset, req.getOffset());
    Assert.assertEquals(length, req.getLength());
  }

  private void assertValid(RPCFileWriteRequest req) {
    try {
      req.validate();
    } catch (Exception e) {
      Assert.fail("request should be valid.");
    }
  }

  private void assertInvalid(RPCFileWriteRequest req) {
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
   * Tests the {@link RPCFileReadRequest#getEncodedLength()} method.
   */
  @Test
  public void encodedLength() {
    RPCFileWriteRequest req = new RPCFileWriteRequest(TEMP_UFS_FILE_ID, OFFSET, LENGTH, null);
    int encodedLength = req.getEncodedLength();
    req.encode(mBuffer);
    Assert.assertEquals(encodedLength, mBuffer.readableBytes());
  }

  /**
   * Tests the {@link RPCFileWriteRequest#encode(ByteBuf)} and
   * {@link RPCFileWriteRequest#decode(ByteBuf)} methods.
   */
  @Test
  public void encodeDecode() {
    RPCFileWriteRequest req = new RPCFileWriteRequest(TEMP_UFS_FILE_ID, OFFSET, LENGTH, null);
    req.encode(mBuffer);
    RPCFileWriteRequest req2 = RPCFileWriteRequest.decode(mBuffer);
    assertValid(TEMP_UFS_FILE_ID, OFFSET, LENGTH, req);
    assertValid(TEMP_UFS_FILE_ID, OFFSET, LENGTH, req2);
  }

  /**
   * Tests the {@link RPCFileWriteRequest#encode(ByteBuf)} and
   * {@link RPCFileWriteRequest#decode(ByteBuf)} methods with data.
   */
  @Test
  public void encodeDecodeData() {
    int length = 10;
    DataBuffer buffer = new DataByteBuffer(ByteBuffer.allocate(length), length);
    RPCFileWriteRequest req = new RPCFileWriteRequest(TEMP_UFS_FILE_ID, OFFSET, length, buffer);
    req.encode(mBuffer);
    mBuffer.writeBytes(buffer.getReadOnlyByteBuffer());
    RPCFileWriteRequest req2 = RPCFileWriteRequest.decode(mBuffer);
    assertValid(TEMP_UFS_FILE_ID, OFFSET, length, req);
    assertValid(TEMP_UFS_FILE_ID, OFFSET, length, req2);
  }

  /**
   * Tests the {@link RPCFileWriteRequest#getPayloadDataBuffer()} method.
   */
  @Test
  public void getPayloadDataBuffer() {
    int length = 10;
    DataByteBuffer payload = new DataByteBuffer(ByteBuffer.allocate(length), length);
    RPCFileWriteRequest req = new RPCFileWriteRequest(TEMP_UFS_FILE_ID, OFFSET, LENGTH, payload);
    assertValid(req);
    Assert.assertEquals(payload, req.getPayloadDataBuffer());
  }

  /**
   * Tests the {@link RPCFileReadRequest#validate()} method.
   */
  @Test
  public void validate() {
    RPCFileWriteRequest req = new RPCFileWriteRequest(TEMP_UFS_FILE_ID, OFFSET, LENGTH, null);
    assertValid(req);
  }

  /**
   * Tests the constructor with a valid length.
   */
  @Test
  public void validLength() {
    RPCFileWriteRequest req = new RPCFileWriteRequest(TEMP_UFS_FILE_ID, OFFSET, LENGTH, null);
    assertValid(req);
    req = new RPCFileWriteRequest(TEMP_UFS_FILE_ID, OFFSET, 0, null);
    assertValid(req);
  }

  /**
   * Tests the constructor with a negative offset.
   */
  @Test
  public void negativeOffset() {
    RPCFileWriteRequest req = new RPCFileWriteRequest(TEMP_UFS_FILE_ID, -1, LENGTH, null);
    assertInvalid(req);
  }

  /**
   * Tests the constructor with an invalid length.
   */
  @Test
  public void invalidLength() {
    RPCFileWriteRequest req = new RPCFileWriteRequest(TEMP_UFS_FILE_ID, OFFSET, -1, null);
    assertInvalid(req);
  }
}
