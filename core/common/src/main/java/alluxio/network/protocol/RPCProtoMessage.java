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
import alluxio.network.protocol.databuffer.DataNettyBuffer;
import alluxio.proto.dataserver.Protocol;

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import io.netty.buffer.ByteBuf;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This is the main base class for all protocol buffer based RPC messages to the DataServer.
 * Each RPC message is composed of a proto message and a data buffer.
 *
 * Encoded format:
 * [proto message length][serialized proto message][data buffer]
 */
@ThreadSafe
public final class RPCProtoMessage extends RPCMessage {
  private final MessageLite mMessage;
  private final byte[] mMessageEncoded;
  private final DataBuffer mData;

  /**
   * Creates an instance of {@link RPCProtoMessage}.
   *
   * @param message the message
   * @param data the data which can be null
   */
  public RPCProtoMessage(MessageLite message, ByteBuf data) {
    mMessage = message;
    mMessageEncoded = message.toByteArray();
    if (data!= null && data.readableBytes() > 0) {
      mData = new DataNettyBuffer(data, data.readableBytes());
    } else {
      mData = null;
    }
  }

  /**
   * Creates an instance of {@link RPCProtoMessage} without data part.
   *
   * @param message the message
   */
  public RPCProtoMessage(MessageLite message) {
    this(message, null);
  }

  /**
   * Creates an instance of {@link RPCProtoMessage} from a serialized proto message.
   *
   * @param serialized the serialized message
   * @param prototype the prototype of the message used to identify the type of the message
   * @param data the data which can be null
   */
  public RPCProtoMessage(byte[] serialized, MessageLite prototype, ByteBuf data) {
    try {
      mMessage = prototype.getParserForType().parseFrom(serialized);
    } catch (InvalidProtocolBufferException e) {
      // Runtime exception will not kill the netty server.
      throw Throwables.propagate(e);
    }
    mMessageEncoded = serialized;
    if (data != null && data.readableBytes() > 0) {
      mData = new DataNettyBuffer(data, data.readableBytes());
    } else {
      mData = null;
    }
  }

  @Override
  public int getEncodedLength() {
    return Ints.BYTES + mMessageEncoded.length;
  }

  @Override
  public void encode(ByteBuf out) {
    out.writeInt(mMessageEncoded.length);
    out.writeBytes(mMessageEncoded);
  }

  public static RPCProtoMessage decode(ByteBuf in, MessageLite prototype) {
    int length = in.readInt();
    byte[] serialized = new byte[length];
    in.readBytes(serialized);
    return new RPCProtoMessage(serialized, prototype, in);
  }

  @Override
  public Type getType() {
    if (mMessage instanceof Protocol.ReadRequest) {
      return Type.RPC_READ_REQUEST;
    }
    if (mMessage instanceof Protocol.WriteRequest) {
      return Type.RPC_WRITE_REQUEST;
    }
    if (mMessage instanceof Protocol.Response) {
      return Type.RPC_RESPONSE;
    }
    return Type.RPC_UNKNOWN;
  }

  @Override
  public void validate() {}

  @Override
  public boolean hasPayload() {
    return getPayloadDataBuffer() != null;
  }

  @Override
  public DataBuffer getPayloadDataBuffer() {
    return mData;
  }

  /**
   * @return the message
   */
  public MessageLite getMessage() {
    return mMessage;
  }
}
