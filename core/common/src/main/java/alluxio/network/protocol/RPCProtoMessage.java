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
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.proto.dataserver.Protocol.Response;
import alluxio.util.proto.ProtoMessage;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This is the main base class for all protocol buffer based RPC messages to the DataServer.
 * Each RPC message is composed of a proto message and a data buffer.
 * <p>
 * Encoded format:
 * [proto message length][serialized proto message][data buffer]
 * The proto message length doesn't include the length of itself (the length field).
 * <p>
 * Note: The data buffer must be released when it is not used. Usually this is how it is released:
 * 1. On the consumer side, a {@link RPCProtoMessage} is decoded and the data buffer is extracted.
 * The ownership of the data buffer is transferred from then on.
 * 2. On the producer side, a {@link RPCProtoMessage} is created. It will be sent on the wire via
 * netty which will take ownership of the data buffer.
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
   * @param data    the data which can be null. Ownership is taken by this class
   */
  public RPCProtoMessage(ProtoMessage message, DataBuffer data) {
    if (data != null) {
      Preconditions
          .checkArgument((data instanceof NettyDataBuffer) || (data instanceof DataFileChannel),
              "Only NettyDataBuffer and DataFileChannel are allowed.");
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
   * @param prototype  the prototype of the message used to identify the type of the message
   * @param data       the data which can be null
   */
  public RPCProtoMessage(byte[] serialized, ProtoMessage prototype, DataBuffer data) {
    Preconditions
        .checkArgument((data instanceof NettyDataBuffer) || (data instanceof DataFileChannel),
            "Only NettyDataBuffer and DataFileChannel are allowed.");
    mMessage = ProtoMessage.parseFrom(serialized, prototype);
    // TODO(JiamingMai): there is a copy operation here, check if we can remove this
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
   * @param in        the buffer
   * @param prototype a message prototype used to infer the type of the message
   * @return the message decoded
   */
  public static RPCProtoMessage decode(ByteBuf in, ProtoMessage prototype) {
    int length = in.readInt();
    byte[] serialized = new byte[length];
    in.readBytes(serialized);
    in.retain();
    return new RPCProtoMessage(serialized, prototype, new NettyDataBuffer(in));
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
    } else if (mMessage.isAsyncCacheRequest()) {
      return Type.RPC_ASYNC_CACHE_REQUEST;
    } else if (mMessage.isHeartbeat()) {
      return Type.RPC_HEARTBEAT;
    } else if (mMessage.isReadResponse()) {
      return Type.RPC_READ_RESPONSE;
    } else {
      return Type.RPC_UNKNOWN;
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
   * @param status  the status code
   * @param message the message
   * @param data    the data buffer
   * @return the created {@link RPCProtoMessage}
   */
  public static RPCProtoMessage createResponse(Status status, String message, DataBuffer data) {
    Response response = Response.newBuilder().setStatus(toProto(status))
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
    return createResponse(Status.CANCELLED, "canceled", null);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("message", mMessage)
        .add("dataLength", mData == null ? 0 : mData.getLength()).toString();
  }

  /**
   * Converts an internal exception status to a protocol buffer type status.
   *
   * @param status the status to convert
   * @return the protocol buffer type status
   */
  public static alluxio.proto.status.Status.PStatus toProto(Status status) {
    switch (status.getCode()) {
      case ABORTED:
        return alluxio.proto.status.Status.PStatus.ABORTED;
      case ALREADY_EXISTS:
        return alluxio.proto.status.Status.PStatus.ALREADY_EXISTS;
      case CANCELLED:
        return alluxio.proto.status.Status.PStatus.CANCELLED;
      case DATA_LOSS:
        return alluxio.proto.status.Status.PStatus.DATA_LOSS;
      case DEADLINE_EXCEEDED:
        return alluxio.proto.status.Status.PStatus.DEADLINE_EXCEEDED;
      case FAILED_PRECONDITION:
        return alluxio.proto.status.Status.PStatus.FAILED_PRECONDITION;
      case INTERNAL:
        return alluxio.proto.status.Status.PStatus.INTERNAL;
      case INVALID_ARGUMENT:
        return alluxio.proto.status.Status.PStatus.INVALID_ARGUMENT;
      case NOT_FOUND:
        return alluxio.proto.status.Status.PStatus.NOT_FOUND;
      case OK:
        return alluxio.proto.status.Status.PStatus.OK;
      case OUT_OF_RANGE:
        return alluxio.proto.status.Status.PStatus.OUT_OF_RANGE;
      case PERMISSION_DENIED:
        return alluxio.proto.status.Status.PStatus.PERMISSION_DENIED;
      case RESOURCE_EXHAUSTED:
        return alluxio.proto.status.Status.PStatus.RESOURCE_EXHAUSTED;
      case UNAUTHENTICATED:
        return alluxio.proto.status.Status.PStatus.UNAUTHENTICATED;
      case UNAVAILABLE:
        return alluxio.proto.status.Status.PStatus.UNAVAILABLE;
      case UNIMPLEMENTED:
        return alluxio.proto.status.Status.PStatus.UNIMPLEMENTED;
      case UNKNOWN:
        return alluxio.proto.status.Status.PStatus.UNKNOWN;
      default:
        return alluxio.proto.status.Status.PStatus.UNKNOWN;
    }
  }

  /**
   * Creates a {@link Status} from a protocol buffer type status.
   *
   * @param status the protocol buffer type status
   * @return the corresponding {@link Status}
   */
  public static Status fromProto(alluxio.proto.status.Status.PStatus status) {
    switch (status) {
      case ABORTED:
        return Status.ABORTED;
      case ALREADY_EXISTS:
        return Status.ALREADY_EXISTS;
      case CANCELLED:
        return Status.CANCELLED;
      case DATA_LOSS:
        return Status.DATA_LOSS;
      case DEADLINE_EXCEEDED:
        return Status.DEADLINE_EXCEEDED;
      case FAILED_PRECONDITION:
        return Status.FAILED_PRECONDITION;
      case INTERNAL:
        return Status.INTERNAL;
      case INVALID_ARGUMENT:
        return Status.INVALID_ARGUMENT;
      case NOT_FOUND:
        return Status.NOT_FOUND;
      case OK:
        return Status.OK;
      case OUT_OF_RANGE:
        return Status.OUT_OF_RANGE;
      case PERMISSION_DENIED:
        return Status.PERMISSION_DENIED;
      case RESOURCE_EXHAUSTED:
        return Status.RESOURCE_EXHAUSTED;
      case UNAUTHENTICATED:
        return Status.UNAUTHENTICATED;
      case UNAVAILABLE:
        return Status.UNAVAILABLE;
      case UNIMPLEMENTED:
        return Status.UNIMPLEMENTED;
      case UNKNOWN:
        return Status.UNKNOWN;
      default:
        return Status.UNKNOWN;
    }
  }
}

