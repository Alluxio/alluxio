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
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.util.proto.ProtoMessage;
import alluxio.proto.dataserver.Protocol;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This is the main base class for all protocol buffer based RPC messages to the DataServer.
 * Each RPC message is composed of a proto message and a data buffer.
 *
 * Encoded format:
 * [proto message length][serialized proto message][data buffer]
 * The proto message length doesn't include the length of itself (the length field).
 *
 * Note: The data buffer must be released when it is not used. Usually this is how it is released:
 * 1. On the consumer side, a {@link RPCProtoMessage} is decoded and the data buffer is extracted.
 *    The ownership of the data buffer is transferred from then on.
 * 2. On the producer side, a {@link RPCProtoMessage} is created. It will be sent on the wire via
 *    netty which will take ownership of the data buffer.
 * Given the above usage patterns, {@link RPCProtoMessage} doesn't provide a 'release' interface
 * to avoid confusing the user.
 */
@ThreadSafe
public final class RPCProtoMessage extends RPCMessage {
  private final ProtoMessage mMessage;
  private final byte[] mMessageEncoded;
  private final DataBuffer mData;

  /**
   * Creates an instance of {@link RPCProtoMessage}.
   *
   * @param message the message
   * @param data the data which can be null. Ownership is taken by this class
   */
  public RPCProtoMessage(ProtoMessage message, DataBuffer data) {
    if (data != null) {
      Preconditions
          .checkArgument((data instanceof DataNettyBufferV2) || (data instanceof DataFileChannel),
              "Only DataNettyBufferV2 and DataFileChannel are allowed.");
    }
    mMessage = message;
    mMessageEncoded = message.toByteArray();
    if (data != null && data.getLength() > 0) {
      mData = data;
    } else if (data != null) {
      data.release();
      mData = null;
    } else {
      mData = null;
    }
  }

  /**
   * Creates an instance of {@link RPCProtoMessage} without data part.
   *
   * @param message the message
   */
  public RPCProtoMessage(ProtoMessage message) {
    this(message, null);
  }

  /**
   * Creates an instance of {@link RPCProtoMessage} from a serialized proto message.
   *
   * @param serialized the serialized message
   * @param prototype the prototype of the message used to identify the type of the message
   * @param data the data which can be null
   */
  public RPCProtoMessage(byte[] serialized, ProtoMessage.Type prototype, DataBuffer data) {
    Preconditions
        .checkArgument((data instanceof DataNettyBufferV2) || (data instanceof DataFileChannel),
            "Only DataNettyBufferV2 and DataFileChannel are allowed.");
    mMessage = ProtoMessage.parseFrom(prototype, serialized);
    mMessageEncoded = Arrays.copyOf(serialized, serialized.length);
    if (data != null && data.getLength() > 0) {
      mData = data;
    } else if (data != null) {
      data.release();
      mData = null;
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

  /**
   * Decodes the message from a buffer. This method increments the refcount of the bytebuf passed
   * by 1.
   *
   * @param in the buffer
   * @param prototype a message prototype used to infer the type of the message
   * @return the message decoded
   */
  public static RPCProtoMessage decode(ByteBuf in, ProtoMessage.Type prototype) {
    int length = in.readInt();
    byte[] serialized = new byte[length];
    in.readBytes(serialized);
    in.retain();
    return new RPCProtoMessage(serialized, prototype, new DataNettyBufferV2(in));
  }

  @Override
  public Type getType() {
    switch (mMessage.getType()) {
      case READ_REQUEST:
        return RPCMessage.Type.RPC_READ_REQUEST;
      case WRITE_REQUEST:
        return RPCMessage.Type.RPC_WRITE_REQUEST;
      case RESPONSE:
        return RPCMessage.Type.RPC_RESPONSE;
      default:
        return RPCMessage.Type.RPC_UNKNOWN;
    }
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
  public ProtoMessage getMessage() {
    return mMessage;
  }

  /**
   * Creates a response for a given status.
   *
   * @param code the status code
   * @param message the user provided message
   * @param e the cause of this error
   * @param data the data buffer
   * @return the message created
   */
  public static RPCProtoMessage createResponse(Protocol.Status.Code code, String message,
      Throwable e, DataBuffer data) {
    Protocol.Status status = Protocol.Status.newBuilder().setCode(code).setMessage(message).build();
    if (e != null) {
      Protocol.Exception.Builder builder = Protocol.Exception.newBuilder();
      String className = e.getClass().getCanonicalName();
      if (className != null) {
        builder.setClassName(className);
      }
      if (e.getMessage() != null) {
        builder.setMessage(e.getMessage());
      }
      status = status.toBuilder().setCause(builder.build()).build();
    }
    Protocol.Response response = Protocol.Response.newBuilder().setStatus(status).build();
    return new RPCProtoMessage(new ProtoMessage(response), data);
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

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("message", mMessage)
        .add("dataLength", mData == null ? 0 : mData.getLength()).toString();
  }
}

