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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NettyDataBuffer;

import com.google.protobuf.ByteString;
import io.netty.buffer.Unpooled;
import org.junit.Test;

/**
 * Unit tests for {@link WriteRequestMarshaller}.
 */
public final class WriteRequestMarshallerTest {

  @Test
  public void close() {
    WriteRequestMarshaller marshaller = new WriteRequestMarshaller();

    WriteRequest msg1 = buildRequest("test1".getBytes());
    marshaller.offerBuffer(new NettyDataBuffer(
        Unpooled.wrappedBuffer(msg1.getChunk().getData().asReadOnlyByteBuffer())), msg1);

    DataBuffer data = marshaller.pollBuffer(msg1);
    assertNotNull(data);
    data.release();

    // close the marshaller
    marshaller.close();

    WriteRequest msg2 = buildRequest("test2".getBytes());
    marshaller.offerBuffer(new NettyDataBuffer(
        Unpooled.wrappedBuffer(msg2.getChunk().getData().asReadOnlyByteBuffer())), msg2);

    // No buffers should not exist, since the marshaller is already closed
    assertNull(marshaller.pollBuffer(msg1));
    assertNull(marshaller.pollBuffer(msg2));
  }

  private WriteRequest buildRequest(byte[] data) {
    return WriteRequest.newBuilder().setChunk(Chunk.newBuilder().setData(
        ByteString.copyFrom(data))).build();
  }
}
