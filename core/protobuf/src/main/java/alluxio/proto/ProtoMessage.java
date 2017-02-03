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

package alluxio.proto;

import alluxio.proto.dataserver.Protocol;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

/**
 * A simple wrapper around Protobuf MessageLite.
 */
public final class ProtoMessage {

  public enum Type {
    READ_REQUEST,
    WRITE_REQUEST,
    RESPONSE,
  }

  private MessageLite mMessage;

  private Type mType;

  public ProtoMessage(Protocol.ReadRequest message) {
    this(message, Type.READ_REQUEST);
  }

  public ProtoMessage(Protocol.WriteRequest message) {
    this(message, Type.WRITE_REQUEST);
  }

  public ProtoMessage(Protocol.Response message) {
    this(message, Type.RESPONSE);
  }

  ProtoMessage(MessageLite message, Type type) {
    mMessage = message;
    mType = type;
  }

  public <T> T getMessage() {
    @SuppressWarnings("unchecked")
    T ret = (T) mMessage;
    return ret;
  }

  public Type getType() {
    return mType;
  }

  public byte[] toByteArray() {
    return mMessage.toByteArray();
  }

  public static ProtoMessage parseFrom(Type type, byte[] serialized) {
    MessageLite message;
    try {
      switch (type) {
        case READ_REQUEST:
          message = Protocol.ReadRequest.parseFrom(serialized);
          break;
        case WRITE_REQUEST:
          message = Protocol.WriteRequest.parseFrom(serialized);
          break;
        case RESPONSE:
          message = Protocol.Response.parseFrom(serialized);
          break;
        default:
          throw new IllegalArgumentException("Unknown class");
      }
      return new ProtoMessage(message, type);
    } catch (InvalidProtocolBufferException e) {
      // Runtime exception will not kill the netty server.
      throw new RuntimeException(e);
    }
  }
}
