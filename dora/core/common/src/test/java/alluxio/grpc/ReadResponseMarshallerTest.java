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

package alluxio.grpc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NettyDataBuffer;

import com.google.protobuf.ByteString;
import io.grpc.Drainable;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Unit tests for {@link ReadResponseMarshaller}.
 */
public final class ReadResponseMarshallerTest {

  @Test
  public void streamEmptyMessage() throws Exception {
    validateStream(ReadResponse.getDefaultInstance());
  }

  @Test
  public void streamMessage() throws Exception {
    validateStream(buildResponse("test".getBytes()));
  }

  @Test
  public void parseEmptyMessage() throws Exception {
    validateParse(ReadResponse.getDefaultInstance());
  }

  @Test
  public void parseMessage() throws Exception {
    validateParse(buildResponse("test".getBytes()));
  }

  @Test
  public void close() {
    ReadResponseMarshaller marshaller = new ReadResponseMarshaller();

    ReadResponse msg1 = buildResponse("test1".getBytes());
    marshaller.offerBuffer(new NettyDataBuffer(
        Unpooled.wrappedBuffer(msg1.getChunk().getData().asReadOnlyByteBuffer())), msg1);

    DataBuffer data = marshaller.pollBuffer(msg1);
    assertNotNull(data);
    data.release();

    // close the marshaller
    marshaller.close();

    ReadResponse msg2 = buildResponse("test2".getBytes());
    marshaller.offerBuffer(new NettyDataBuffer(
        Unpooled.wrappedBuffer(msg2.getChunk().getData().asReadOnlyByteBuffer())), msg2);

    // No buffers should not exist, since the marshaller is already closed
    assertNull(marshaller.pollBuffer(msg1));
    assertNull(marshaller.pollBuffer(msg2));
  }

  private void validateStream(ReadResponse message) throws IOException {
    ReadResponseMarshaller marshaller = new ReadResponseMarshaller();
    byte[] expected = message.toByteArray();
    if (message.hasChunk() && message.getChunk().hasData()) {
      marshaller.offerBuffer(new NettyDataBuffer(
          Unpooled.wrappedBuffer(message.getChunk().getData().asReadOnlyByteBuffer())), message);
    }
    InputStream stream = marshaller.stream(message);
    assertTrue(stream instanceof Drainable);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ((Drainable) stream).drainTo(outputStream);
    assertArrayEquals(expected, outputStream.toByteArray());
  }

  private void validateParse(ReadResponse message) {
    ReadResponseMarshaller marshaller = new ReadResponseMarshaller();
    byte[] data = message.toByteArray();
    ReadResponse parsedMessage = marshaller.parse(new ByteArrayInputStream(data));
    if (data.length > 0) {
      DataBuffer buffer = marshaller.pollBuffer(parsedMessage);
      assertNotNull(buffer);
      byte[] bytes = new byte[buffer.readableBytes()];
      buffer.readBytes(bytes, 0, bytes.length);
      parsedMessage = parsedMessage.toBuilder().setChunk(Chunk.newBuilder().setData(
        ByteString.copyFrom(bytes)
      ).build()).build();
    }
    assertEquals(message, parsedMessage);
  }

  private ReadResponse buildResponse(byte[] data) {
    return ReadResponse.newBuilder().setChunk(Chunk.newBuilder().setData(
        ByteString.copyFrom(data))).build();
  }
}
