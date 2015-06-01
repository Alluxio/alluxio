/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.netty.protocol;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import tachyon.worker.netty.protocol.buffer.DataBuffer;

// This is the main base class for all RPC messages to the DataServer.
// The message and type encoding scheme is adapted from the implementation found in the streaming
// server in Apache Spark.
public abstract class RPCMessage implements EncodedMessage {

  // The possible types of RPC messages.
  public enum Type implements EncodedMessage {
    RPC_BLOCK_REQUEST(0), RPC_BLOCK_RESPONSE(1);

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
     * Returns the type represented by the id from the input ByteBuf.
     *
     * This must be updated to add new message types.
     *
     * @param in The input ByteBuf to decode into a type
     * @return The decoded message type
     */
    public static Type decode(ByteBuf in) {
      int id = in.readInt();
      switch (id) {
        case 0:
          return RPC_BLOCK_REQUEST;
        case 1:
          return RPC_BLOCK_RESPONSE;
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
   * @return true if the message has a payload, false otherwise.
   */
  public boolean hasPayload() {
    return getPayloadDataBuffer() != null;
  }

  /**
   * Returns the data buffer of the payload.
   *
   * @return The DataBuffer representing the payload.
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
   * @param in the input ByteBuf
   * @return the decoded RPCMessage
   */
  public static RPCMessage decodeMessage(RPCMessage.Type type, ByteBuf in) {
    switch (type) {
      case RPC_BLOCK_REQUEST:
        return RPCBlockRequest.decode(in);
      case RPC_BLOCK_RESPONSE:
        return RPCBlockResponse.decode(in);
      default:
        throw new IllegalArgumentException("Unknown RPCMessage type. type: " + type);
    }
  }

}
