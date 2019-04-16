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

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import com.google.protobuf.WireFormat;
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.ReadableBuffers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Marshaller for {@link ReadResponse}.
 */
@NotThreadSafe
public class ReadResponseMarshaller extends DataMessageMarshaller<ReadResponse> {
  /**
   * Creates a {@link ReadResponseMarshaller}.
   */
  public ReadResponseMarshaller() {
    super(BlockWorkerGrpc.getReadBlockMethod().getResponseMarshaller());
  }

  @Override
  protected ByteBuf[] serialize(ReadResponse message) throws IOException {
    DataBuffer chunkBuffer = pollBuffer(message);
    if (chunkBuffer == null) {
      if (!message.hasChunk() || !message.getChunk().hasData()) {
        // nothing to serialize
        return new ByteBuf[0];
      }
      // attempts to fallback to read chunk from message
      chunkBuffer = new NettyDataBuffer(
          Unpooled.wrappedBuffer(message.getChunk().getData().asReadOnlyByteBuffer()));
    }
    int size = message.getSerializedSize() - chunkBuffer.readableBytes();
    byte[] header = new byte[size];
    CodedOutputStream stream = CodedOutputStream.newInstance(header);
    stream.writeTag(ReadResponse.CHUNK_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    stream.writeUInt32NoTag(message.getChunk().getSerializedSize());
    stream.writeTag(Chunk.DATA_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    stream.writeUInt32NoTag(chunkBuffer.readableBytes());
    return new ByteBuf[] { Unpooled.wrappedBuffer(header), (ByteBuf) chunkBuffer.getNettyOutput() };
  }

  @Override
  protected ReadResponse deserialize(ReadableBuffer buffer) throws IOException {
    if (buffer.readableBytes() == 0) {
      return ReadResponse.getDefaultInstance();
    }
    try (InputStream is = ReadableBuffers.openStream(buffer, false)) {
      Preconditions.checkState(ProtoUtils.readRawVarint32(is) == GrpcSerializationUtils.makeTag(
          ReadResponse.CHUNK_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED));
      int messageSize = ProtoUtils.readRawVarint32(is);
      Preconditions.checkState(messageSize == buffer.readableBytes());
      Preconditions.checkState(ProtoUtils.readRawVarint32(is) == GrpcSerializationUtils.makeTag(
          Chunk.DATA_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED));
      int chunkSize = ProtoUtils.readRawVarint32(is);
      Preconditions.checkState(chunkSize == buffer.readableBytes());
      ReadResponse response = ReadResponse.newBuilder().build();
      offerBuffer(new ReadableDataBuffer(buffer), response);
      return response;
    }
  }

  @Override
  public ReadResponse combineData(DataMessage<ReadResponse, DataBuffer> message) {
    if (message == null) {
      return null;
    }
    DataBuffer buffer = message.getBuffer();
    if (buffer == null) {
      return message.getMessage();
    }
    try {
      byte[] bytes = new byte[buffer.readableBytes()];
      buffer.readBytes(bytes, 0, bytes.length);
      return message.getMessage().toBuilder()
          .setChunk(Chunk.newBuilder().setData(UnsafeByteOperations.unsafeWrap(bytes)).build())
          .build();
    } finally {
      message.getBuffer().release();
    }
  }
}
