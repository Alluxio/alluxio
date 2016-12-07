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
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.DataNettyBuffer;
import alluxio.proto.dataserver.Protocol;

import com.google.common.base.Preconditions;
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
 *
 * Note: The data buffer must be released when it is not used. Usually this is how it is released:
 * 1. On the server side, a {@link RPCProtoMessage} is decoded and the data buffer is extracted.
 *    The ownership of the data buffer is transferred from then on.
 * 2. On the client side, a {@link RPCProtoMessage} is created. It will be sent on the wire via
 *    netty which will take ownership of the data buffer.
 * Given the above usage patterns, {@link RPCProtoMessage} doesn't provide a 'release' interface
 * to avoid confusing the user.
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
   * @param data the data which can be null. Ownership is taken by this class.
   */
  public RPCProtoMessage(MessageLite message, DataBuffer data) {
    Preconditions
        .checkArgument((data instanceof DataNettyBuffer) || (data instanceof DataFileChannel),
            "Only DataNettyBuffer and DataFileChannel are allowed.");
    mMessage = message;
    mMessageEncoded = message.toByteArray();
    if (data != null && data.getLength() > 0) {
      mData = data;
    } else {
      data.release();
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
  public RPCProtoMessage(byte[] serialized, MessageLite prototype, DataBuffer data) {
    Preconditions
        .checkArgument((data instanceof DataNettyBuffer) || (data instanceof DataFileChannel),
            "Only DataNettyBuffer and DataFileChannel are allowed.");
    try {
      mMessage = prototype.getParserForType().parseFrom(serialized);
    } catch (InvalidProtocolBufferException e) {
      // Runtime exception will not kill the netty server.
      throw Throwables.propagate(e);
    }
    mMessageEncoded = serialized;
    if (data != null && data.getLength() > 0) {
      mData = data;
    } else {
      data.release();
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

  /**
   * Decodes the message from a buffer.
   *
   * @param in the buffer
   * @param prototype a message prototype used to infer the type of the message
   * @return the message decoded
   */
  public static RPCProtoMessage decode(ByteBuf in, MessageLite prototype) {
    int length = in.readInt();
    byte[] serialized = new byte[length];
    in.readBytes(serialized);
    return new RPCProtoMessage(serialized, prototype, new DataNettyBuffer(in, in.readableBytes()));
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
  public void validate() {
  }

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

  /**
   * Creates a response for a given status.
   *
   * @param code the status code
   * @param message the user provided message
   * @param e the cause of this error
   * @return the message created
   */
  public static RPCProtoMessage createResponse(Protocol.Status.Code code, String message,
      Throwable e, DataBuffer data) {
    Protocol.Status status = Protocol.Status.newBuilder().setCode(code).setMessage(message).build();
    if (e != null) {
      Protocol.Exception exception =
          Protocol.Exception.newBuilder().setClassName(e.getClass().getCanonicalName())
              .setMessage(e.getMessage()).build();
      status = status.toBuilder().setCause(exception).build();
    }
    Protocol.Response response = Protocol.Response.newBuilder().setStatus(status).build();
    return new RPCProtoMessage(response, data);
  }

  /**
   * Creates an OK response with data.
   *
   * @param data the data
   * @return the message created
   */
  public static RPCProtoMessage createOkResponse(DataBuffer data) {
    return createResponse(Protocol.Status.Code.OK, "", null, data);
  }
}

