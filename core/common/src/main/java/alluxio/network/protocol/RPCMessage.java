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
import alluxio.proto.dataserver.Protocol;
import alluxio.util.proto.ProtoMessage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This is the main base class for all RPC messages to the DataServer. The message and type encoding
 * scheme is adapted from the implementation found in the streaming server in Apache Spark.
 */
@ThreadSafe
public abstract class RPCMessage implements EncodedMessage {

  /**
   * The possible types of RPC messages.
   */
  public enum Type implements EncodedMessage {
    RPC_ERROR_RESPONSE(0),
    RPC_BLOCK_READ_REQUEST(1),
    RPC_BLOCK_READ_RESPONSE(2),
    RPC_BLOCK_WRITE_REQUEST(3),
    RPC_BLOCK_WRITE_RESPONSE(4),
    RPC_FILE_READ_REQUEST(5),
    RPC_FILE_READ_RESPONSE(6),
    RPC_FILE_WRITE_REQUEST(7),
    RPC_FILE_WRITE_RESPONSE(8),

    // Tags lower than 100 are reserved since v1.4.0.
    RPC_READ_REQUEST(100),
    RPC_WRITE_REQUEST(101),
    RPC_RESPONSE(102),
    RPC_UFS_BLOCK_READ_REQUEST(103),
    RPC_HEARTBEAT(104),
    RPC_LOCAL_BLOCK_OPEN_REQUEST(105),
    RPC_LOCAL_BLOCK_OPEN_RESPONSE(106),
    RPC_LOCAL_BLOCK_CLOSE_REQUEST(107),
    RPC_LOCAL_BLOCK_CREATE_REQUEST(108),
    RPC_LOCAL_BLOCK_CREATE_RESPONSE(109),
    RPC_LOCAL_BLOCK_COMPLETE_REQUEST(110),
    RPC_READ_RESPONSE(111),

    RPC_UNKNOWN(1000),
    ;

    private final int mId;

    Type(int id) {
      mId = id;
    }

    @Override
    public int getEncodedLength() {
      return Ints.BYTES;
    }

    @Override
    public void encode(ByteBuf out) {
      out.writeInt(mId);
    }

    /**
     * Returns the int identifier of the type.
     *
     * Note: This is only used for getting the int representation of the type for
     * {@link alluxio.worker.DataServerMessage}, since that class needs to manually encode all
     * messages. {@link alluxio.worker.DataServerMessage} and this method should no longer be needed
     * when the client is converted to use Netty.
     *
     * @return the int id of the type
     */
    public int getId() {
      return mId;
    }

    /**
     * Returns the type represented by the id from the input ByteBuf.
     *
     * This must be updated to add new message types.
     *
     * @param in The input {@link ByteBuf} to decode into a type
     * @return The decoded message type
     */
    public static Type decode(ByteBuf in) {
      int id = in.readInt();
      switch (id) {
        case 0:
          return RPC_ERROR_RESPONSE;
        case 1:
          return RPC_BLOCK_READ_REQUEST;
        case 2:
          return RPC_BLOCK_READ_RESPONSE;
        case 3:
          return RPC_BLOCK_WRITE_REQUEST;
        case 4:
          return RPC_BLOCK_WRITE_RESPONSE;
        case 5:
          return RPC_FILE_READ_REQUEST;
        case 6:
          return RPC_FILE_READ_RESPONSE;
        case 7:
          return RPC_FILE_WRITE_REQUEST;
        case 8:
          return RPC_FILE_WRITE_RESPONSE;
        case 100:
          return RPC_READ_REQUEST;
        case 101:
          return RPC_WRITE_REQUEST;
        case 102:
          return RPC_RESPONSE;
        case 103:
          return RPC_UFS_BLOCK_READ_REQUEST;
        case 104:
          return RPC_HEARTBEAT;
        case 105:
          return RPC_LOCAL_BLOCK_OPEN_REQUEST;
        case 106:
          return RPC_LOCAL_BLOCK_OPEN_RESPONSE;
        case 107:
          return RPC_LOCAL_BLOCK_CLOSE_REQUEST;
        case 108:
          return RPC_LOCAL_BLOCK_CREATE_REQUEST;
        case 109:
          return RPC_LOCAL_BLOCK_CREATE_RESPONSE;
        case 110:
          return RPC_LOCAL_BLOCK_COMPLETE_REQUEST;
        case 111:
          return RPC_READ_RESPONSE;
        default:
          throw new IllegalArgumentException("Unknown RPCMessage type id. id: " + id);
      }
    }
  }

  /**
   * Returns the type of the message.
   *
   * @return the message type
   */
  public abstract Type getType();

  /**
   * Validate the message. Throws an Exception if the message is invalid.
   */
  public void validate() {}

  /**
   * Returns true if the message has a payload. The encoder will send the payload with a more
   * efficient method.
   *
   * @return true if the message has a payload, false otherwise
   */
  public boolean hasPayload() {
    return getPayloadDataBuffer() != null;
  }

  /**
   * Returns the data buffer of the payload.
   *
   * @return The DataBuffer representing the payload
   */
  public DataBuffer getPayloadDataBuffer() {
    return null;
  }

  /**
   * Creates a decoder that splits up the incoming ByteBuf into new ByteBuf's according to a length
   * field in the input.
   *
   * The encoding scheme is: [(long) frame length][message payload]
   * The frame length is NOT included in the output ByteBuf.
   *
   * @return the frame decoder for Netty
   */
  public static ByteToMessageDecoder createFrameDecoder() {
    // maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip
    return new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Longs.BYTES, -Longs.BYTES,
        Longs.BYTES);
  }

  /**
   * Returns the message of message type 'type', decoded from the input ByteBuf.
   *
   * This must be updated to add new message types.
   *
   * @param type The type of message to decode
   * @param in the input {@link ByteBuf}
   * @return the decoded RPCMessage
   */
  public static RPCMessage decodeMessage(Type type, ByteBuf in) {
    switch (type) {
      case RPC_ERROR_RESPONSE:
        return RPCErrorResponse.decode(in);
      case RPC_BLOCK_READ_REQUEST:
        return RPCBlockReadRequest.decode(in);
      case RPC_BLOCK_READ_RESPONSE:
        return RPCBlockReadResponse.decode(in);
      case RPC_BLOCK_WRITE_REQUEST:
        return RPCBlockWriteRequest.decode(in);
      case RPC_BLOCK_WRITE_RESPONSE:
        return RPCBlockWriteResponse.decode(in);
      case RPC_FILE_READ_REQUEST:
        return RPCFileReadRequest.decode(in);
      case RPC_FILE_READ_RESPONSE:
        return RPCFileReadResponse.decode(in);
      case RPC_FILE_WRITE_REQUEST:
        return RPCFileWriteRequest.decode(in);
      case RPC_FILE_WRITE_RESPONSE:
        return RPCFileWriteResponse.decode(in);
      case RPC_READ_REQUEST:
        return RPCProtoMessage
            .decode(in, new ProtoMessage(Protocol.ReadRequest.getDefaultInstance()));
      case RPC_WRITE_REQUEST:
        return RPCProtoMessage
            .decode(in, new ProtoMessage(Protocol.WriteRequest.getDefaultInstance()));
      case RPC_RESPONSE:
        return RPCProtoMessage.decode(in, new ProtoMessage(Protocol.Response.getDefaultInstance()));
      case RPC_UFS_BLOCK_READ_REQUEST:
        return RPCUnderFileSystemBlockReadRequest.decode(in);
      case RPC_LOCAL_BLOCK_OPEN_REQUEST:
        return RPCProtoMessage
            .decode(in, new ProtoMessage(Protocol.LocalBlockOpenRequest.getDefaultInstance()));
      case RPC_LOCAL_BLOCK_OPEN_RESPONSE:
        return RPCProtoMessage
            .decode(in, new ProtoMessage(Protocol.LocalBlockOpenResponse.getDefaultInstance()));
      case RPC_LOCAL_BLOCK_CLOSE_REQUEST:
        return RPCProtoMessage
            .decode(in, new ProtoMessage(Protocol.LocalBlockCloseRequest.getDefaultInstance()));
      case RPC_LOCAL_BLOCK_CREATE_REQUEST:
        return RPCProtoMessage
            .decode(in, new ProtoMessage(Protocol.LocalBlockCreateRequest.getDefaultInstance()));
      case RPC_LOCAL_BLOCK_CREATE_RESPONSE:
        return RPCProtoMessage
            .decode(in, new ProtoMessage(Protocol.LocalBlockCreateResponse.getDefaultInstance()));
      case RPC_LOCAL_BLOCK_COMPLETE_REQUEST:
        return RPCProtoMessage
            .decode(in, new ProtoMessage(Protocol.LocalBlockCompleteRequest.getDefaultInstance()));
      case RPC_HEARTBEAT:
        return
            RPCProtoMessage.decode(in, new ProtoMessage(Protocol.Heartbeat.getDefaultInstance()));
      case RPC_READ_RESPONSE:
        return RPCProtoMessage
            .decode(in, new ProtoMessage(Protocol.ReadResponse.getDefaultInstance()));
      default:
        throw new IllegalArgumentException("Unknown RPCMessage type. type: " + type);
    }
  }
}
