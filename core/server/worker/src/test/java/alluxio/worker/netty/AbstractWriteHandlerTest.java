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

package alluxio.worker.netty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.status.Status.PStatus;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.proto.ProtoMessage;

import com.google.common.base.Function;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

/**
 * Unit tests for {@link AbstractWriteHandler}.
 */
public abstract class AbstractWriteHandlerTest {
  private static final Random RANDOM = new Random();
  protected static final int PACKET_SIZE = 1024;
  protected static final long TEST_BLOCK_ID = 1L;
  protected static final long TEST_MOUNT_ID = 10L;
  protected EmbeddedChannel mChannel;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Test
  public void writeEmptyFile() throws Exception {
    mChannel.writeInbound(newEofRequest(0));

    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.OK, writeResponse);
  }

  @Test
  public void writeNonEmptyFile() throws Exception {
    long len = 0;
    long checksum = 0;
    for (int i = 0; i < 128; i++) {
      DataBuffer dataBuffer = newDataBuffer(PACKET_SIZE);
      checksum += getChecksum(dataBuffer);
      mChannel.writeInbound(newWriteRequest(len, dataBuffer));
      len += PACKET_SIZE;
    }
    // EOF.
    mChannel.writeInbound(newEofRequest(len));

    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.OK, writeResponse);
    checkWriteData(checksum, len);
  }

  @Test
  public void cancel() throws Exception {
    long len = 0;
    long checksum = 0;
    for (int i = 0; i < 1; i++) {
      DataBuffer dataBuffer = newDataBuffer(PACKET_SIZE);
      checksum += getChecksum(dataBuffer);
      mChannel.writeInbound(newWriteRequest(len, dataBuffer));
      len += PACKET_SIZE;
    }
    // Cancel.
    mChannel.writeInbound(newCancelRequest(len));

    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.CANCELED, writeResponse);
    // Our current implementation does not really abort the file when the write is cancelled.
    // The client issues another request to block worker to abort it.
    checkWriteData(checksum, len);
  }

  @Test
  public void writeInvalidOffsetFirstRequest() throws Exception {
    // The write request contains an invalid offset
    mChannel.writeInbound(newWriteRequest(1, newDataBuffer(PACKET_SIZE)));
    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.INVALID_ARGUMENT, writeResponse);
  }

  @Test
  public void writeInvalidOffsetLaterRequest() throws Exception {
    mChannel.writeInbound(newWriteRequest(0, newDataBuffer(PACKET_SIZE)));
    // The write request contains an invalid offset
    mChannel.writeInbound(newWriteRequest(PACKET_SIZE + 1, newDataBuffer(PACKET_SIZE)));
    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.INVALID_ARGUMENT, writeResponse);
  }

  @Test
  public void writeTwoRequests() throws Exception {
    // Send first request
    mChannel.writeInbound(newWriteRequest(0, newDataBuffer(PACKET_SIZE)));
    mChannel.writeInbound(newEofRequest(PACKET_SIZE));
    // Wait the first packet to finish
    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.OK, writeResponse);
    // Send second request
    mChannel.writeInbound(newWriteRequest(0, newDataBuffer(PACKET_SIZE + 1)));
    mChannel.writeInbound(newEofRequest(PACKET_SIZE + 1));
    writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.OK, writeResponse);
  }

  @Test
  public void writeCancelAndRequests() throws Exception {
    // Send first request
    mChannel.writeInbound(newWriteRequest(0, newDataBuffer(PACKET_SIZE)));
    mChannel.writeInbound(newCancelRequest(PACKET_SIZE));
    // Wait the first packet to finish
    Object writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.CANCELED, writeResponse);
    // Send second request
    mChannel.writeInbound(newWriteRequest(0, newDataBuffer(PACKET_SIZE + 1)));
    mChannel.writeInbound(newEofRequest(PACKET_SIZE + 1));
    writeResponse = waitForResponse(mChannel);
    checkWriteResponse(PStatus.OK, writeResponse);
  }

  @Test
  public void UnregisteredChannelFired() throws Exception {
    ChannelPipeline p = mChannel.pipeline();
    p.fireChannelUnregistered();
  }

  @Test
  public void UnregisteredChannelFiredAfterRequest() throws Exception {
    mChannel.writeInbound(newEofRequest(0));
    ChannelPipeline p = mChannel.pipeline();
    p.fireChannelUnregistered();
  }

  /**
   * Checks the given write response is expected and matches the given error code.
   *
   * @param expectedStatus the expected status code
   * @param writeResponse the write response
   */
  protected void checkWriteResponse(PStatus expectedStatus, Object writeResponse) {
    assertTrue(writeResponse instanceof RPCProtoMessage);
    ProtoMessage response = ((RPCProtoMessage) writeResponse).getMessage();
    assertTrue(response.isResponse());
    assertEquals(expectedStatus, response.asResponse().getStatus());
  }

  /**
   * Checks the file content matches expectation (file length and file checksum).
   *
   * @param expectedChecksum the expected checksum of the file
   * @param size the file size in bytes
   */
  protected void checkWriteData(long expectedChecksum, long size) throws IOException {
    long actualChecksum = 0;
    long actualSize = 0;

    byte[] buffer = new byte[(int) Math.min(Constants.KB, size)];
    try (InputStream input = getWriteDataStream()) {
      int bytesRead;
      while ((bytesRead = input.read(buffer)) >= 0) {
        for (int i = 0; i < bytesRead; i++) {
          actualChecksum += BufferUtils.byteToInt(buffer[i]);
          actualSize++;
        }
      }
    }

    assertEquals(expectedChecksum, actualChecksum);
    assertEquals(size, actualSize);
  }

  /**
   * Waits for a response.
   *
   * @return the response
   */
  protected Object waitForResponse(final EmbeddedChannel channel) {
    return CommonUtils.waitForResult("response from the channel.", new Function<Void, Object>() {
      @Override
      public Object apply(Void v) {
        return channel.readOutbound();
      }
    }, WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
  }

  protected Protocol.WriteRequest newWriteRequestProto(long offset) {
    return Protocol.WriteRequest.newBuilder().setId(TEST_BLOCK_ID).setOffset(offset)
        .setType(getWriteRequestType()).build();
  }

  /**
   * Builds the write request.
   *
   * @param offset the offset
   * @param buffer the data to write
   * @return the write request
   */
  protected RPCProtoMessage newWriteRequest(long offset, DataBuffer buffer) {
    Protocol.WriteRequest writeRequest = newWriteRequestProto(offset);
    return new RPCProtoMessage(new ProtoMessage(writeRequest), buffer);
  }

  /**
   * @param offset the offset
   *
   * @return a new Eof write request
   */
  protected RPCProtoMessage newEofRequest(long offset) {
    Protocol.WriteRequest writeRequest =
        newWriteRequestProto(offset).toBuilder().setEof(true).build();
    return new RPCProtoMessage(new ProtoMessage(writeRequest), null);
  }

  /**
   * @param offset the offset
   *
   * @return a new cancel write request
   */
  protected RPCProtoMessage newCancelRequest(long offset) {
    Protocol.WriteRequest writeRequest =
        newWriteRequestProto(offset).toBuilder().setCancel(true).build();
    return new RPCProtoMessage(new ProtoMessage(writeRequest), null);
  }

  /**
   * @return the write type of the request
   */
  protected abstract Protocol.RequestType getWriteRequestType();

  /**
   * @return the data written as an InputStream
   */
  protected abstract InputStream getWriteDataStream() throws IOException;

  /**
   * @param len length of the data buffer
   * @return a newly created data buffer
   */
  protected DataBuffer newDataBuffer(int len) {
    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(len);
    for (int i = 0; i < len; i++) {
      byte value = (byte) (RANDOM.nextInt() % Byte.MAX_VALUE);
      buf.writeByte(value);
    }
    return new DataNettyBufferV2(buf);
  }

  /**
   * @param buffer buffer to get checksum
   * @return the checksum
   */
  public static long getChecksum(DataBuffer buffer) {
    return getChecksum((ByteBuf) buffer.getNettyOutput());
  }

  /**
   * @param buffer buffer to get checksum
   * @return the checksum
   */
  public static long getChecksum(ByteBuf buffer) {
    long ret = 0;
    for (int i = 0; i < buffer.capacity(); i++) {
      ret += BufferUtils.byteToInt(buffer.getByte(i));
    }
    return ret;
  }
}
