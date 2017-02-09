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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link RPCFileReadRequestTest}.
 */
public class RPCFileReadRequestTest {
  private static final long TEMP_UFS_FILE_ID = 11;
  private static final long OFFSET = 22;
  private static final long LENGTH = 33;

  private ByteBuf mBuffer = null;

  private void assertValid(long tempUfsFileId, long offset, long length, RPCFileReadRequest req) {
    Assert.assertEquals(RPCMessage.Type.RPC_FILE_READ_REQUEST, req.getType());
    Assert.assertEquals(tempUfsFileId, req.getTempUfsFileId());
    Assert.assertEquals(offset, req.getOffset());
    Assert.assertEquals(length, req.getLength());
  }

  private void assertValid(RPCFileReadRequest req) {
    try {
      req.validate();
    } catch (Exception e) {
      Assert.fail("request should be valid.");
    }
  }

  private void assertInvalid(RPCFileReadRequest req) {
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
    RPCFileReadRequest req = new RPCFileReadRequest(TEMP_UFS_FILE_ID, OFFSET, LENGTH);
    int encodedLength = req.getEncodedLength();
    req.encode(mBuffer);
    Assert.assertEquals(encodedLength, mBuffer.readableBytes());
  }

  /**
   * Tests the {@link RPCFileReadRequest#encode(ByteBuf)} and
   * {@link RPCFileReadRequest#decode(ByteBuf)} methods.
   */
  @Test
  public void encodeDecode() {
    RPCFileReadRequest req = new RPCFileReadRequest(TEMP_UFS_FILE_ID, OFFSET, LENGTH);
    req.encode(mBuffer);
    RPCFileReadRequest req2 = RPCFileReadRequest.decode(mBuffer);
    assertValid(TEMP_UFS_FILE_ID, OFFSET, LENGTH, req);
    assertValid(TEMP_UFS_FILE_ID, OFFSET, LENGTH, req2);
  }

  /**
   * Tests the {@link RPCFileReadRequest#validate()} method.
   */
  @Test
  public void validate() {
    RPCFileReadRequest req = new RPCFileReadRequest(TEMP_UFS_FILE_ID, OFFSET, LENGTH);
    assertValid(req);
  }

  /**
   * Tests the {@link RPCFileReadRequest#RPCFileReadRequest(long, long, long)} constructor with a
   * valid length.
   */
  @Test
  public void validLength() {
    RPCFileReadRequest req = new RPCFileReadRequest(TEMP_UFS_FILE_ID, OFFSET, LENGTH);
    assertValid(req);
    req = new RPCFileReadRequest(TEMP_UFS_FILE_ID, OFFSET, 0);
    assertValid(req);
  }

  /**
   * Tests the {@link RPCFileReadRequest#RPCFileReadRequest(long, long, long)} constructor with a
   * negative offset.
   */
  @Test
  public void negativeOffset() {
    RPCFileReadRequest req = new RPCFileReadRequest(TEMP_UFS_FILE_ID, -1, LENGTH);
    assertInvalid(req);
  }

  /**
   * Tests the {@link RPCFileReadRequest#RPCFileReadRequest(long, long, long)}
   * constructor with an invalid length.
   */
  @Test
  public void invalidLength() {
    RPCFileReadRequest req = new RPCFileReadRequest(TEMP_UFS_FILE_ID, OFFSET, -100);
    assertInvalid(req);
  }
}
