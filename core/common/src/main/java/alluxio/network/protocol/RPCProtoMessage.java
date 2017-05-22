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

import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.Status;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.dataserver.Protocol.Response;
import alluxio.util.proto.ProtoMessage;

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
  public RPCProtoMessage(byte[] serialized, ProtoMessage prototype, DataBuffer data) {
    Preconditions
        .checkArgument((data instanceof DataNettyBufferV2) || (data instanceof DataFileChannel),
            "Only DataNettyBufferV2 and DataFileChannel are allowed.");
    mMessage = ProtoMessage.parseFrom(serialized, prototype);
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
  public static RPCProtoMessage decode(ByteBuf in, ProtoMessage prototype) {
    int length = in.readInt();
    byte[] serialized = new byte[length];
    in.readBytes(serialized);
    in.retain();
    return new RPCProtoMessage(serialized, prototype, new DataNettyBufferV2(in));
  }

  @Override
  public Type getType() {
    if (mMessage.isReadRequest()) {
      return Type.RPC_READ_REQUEST;
    } else if (mMessage.isWriteRequest()) {
      return Type.RPC_WRITE_REQUEST;
    } else if (mMessage.isResponse()) {
      return Type.RPC_RESPONSE;
    } else if (mMessage.isLocalBlockOpenRequest()) {
      return Type.RPC_LOCAL_BLOCK_OPEN_REQUEST;
    } else if (mMessage.isLocalBlockOpenResponse()) {
      return Type.RPC_LOCAL_BLOCK_OPEN_RESPONSE;
    } else if (mMessage.isLocalBlockCloseRequest()) {
      return Type.RPC_LOCAL_BLOCK_CLOSE_REQUEST;
    } else if (mMessage.isLocalBlockCreateRequest()) {
      return Type.RPC_LOCAL_BLOCK_CREATE_REQUEST;
    } else if (mMessage.isLocalBlockCreateResponse()) {
      return Type.RPC_LOCAL_BLOCK_CREATE_RESPONSE;
    } else if (mMessage.isLocalBlockCompleteRequest()) {
      return Type.RPC_LOCAL_BLOCK_COMPLETE_REQUEST;
    } else if (mMessage.isHeartbeat()) {
      return Type.RPC_HEARTBEAT;
    } else if (mMessage.isReadResponse()) {
      return Type.RPC_READ_RESPONSE;
    } else {
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
   * Creates a response for a given {@link AlluxioStatusException}.
   *
   * @param se the {@link AlluxioStatusException}
   * @return the created {@link RPCProtoMessage}
   */
  public static RPCProtoMessage createResponse(AlluxioStatusException se) {
    String message = se.getMessage() != null ? se.getMessage() : "";
    return createResponse(se.getStatus(), message, null);
  }

  /**
   * Creates a response for a given status, message, and data buffer.
   *
   * @param status the status code
   * @param message the message
   * @param data the data buffer
   * @return the created {@link RPCProtoMessage}
   */
  public static RPCProtoMessage createResponse(Status status, String message, DataBuffer data) {
    Response response = Protocol.Response.newBuilder().setStatus(Status.toProto(status))
        .setMessage(message).build();
    return new RPCProtoMessage(new ProtoMessage(response), data);
  }

  /**
   * Creates an OK response with data.
   *
   * @param data the data
   * @return the message created
   */
  public static RPCProtoMessage createOkResponse(DataBuffer data) {
    return createResponse(Status.OK, "", data);
  }

  /**
   * Creates a response in CANCELLED state.
   *
   * @return the message created
   */
  public static RPCProtoMessage createCancelResponse() {
    return createResponse(Status.CANCELED, "canceled", null);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("message", mMessage)
        .add("dataLength", mData == null ? 0 : mData.getLength()).toString();
  }
}

