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

package alluxio.util.proto;

import alluxio.proto.dataserver.Protocol;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

/**
 * A simple wrapper around the MessageLite class in Protobuf for a few messages defined and
 * generated in Alluxio. In other parts of Alluxio code base that are outside of this module,
 * use this class to replace MessageLite when it must reference MessageLite as a base class of
 * different generated messages. This class is intended to be used internally only.
 */
public final class ProtoMessage {

  /**
   * Type of the message.
   */
  public enum Type {
    READ_REQUEST,
    WRITE_REQUEST,
    RESPONSE,
  }

  private MessageLite mMessage;
  private Type mType;

  /**
   * Constructs a {@link ProtoMessage} instance wrapping around {@link Protocol.ReadRequest}.
   *
   * @param message the message to wrap
   */
  public ProtoMessage(Protocol.ReadRequest message) {
    this(message, Type.READ_REQUEST);
  }

  /**
   * Constructs a {@link ProtoMessage} instance wrapping around {@link Protocol.WriteRequest}.
   *
   * @param message the message to wrap
   */
  public ProtoMessage(Protocol.WriteRequest message) {
    this(message, Type.WRITE_REQUEST);
  }

  /**
   * Constructs a {@link ProtoMessage} instance wrapping around {@link Protocol.Response}.
   *
   * @param message the message to wrap
   */
  public ProtoMessage(Protocol.Response message) {
    this(message, Type.RESPONSE);
  }

  /**
   * Constructs a {@link ProtoMessage} instance wrapping around {@link MessageLite}.
   *
   * @param message the message to wrap
   * @param type type of the message
   */
  public ProtoMessage(MessageLite message, Type type) {
    mMessage = message;
    mType = type;
  }

  /**
   * @param <T> the type T
   *
   * @return the unwrapped message as type T
   */
  public <T> T getMessage() {
    @SuppressWarnings("unchecked")
    T ret = (T) mMessage;
    return ret;
  }

  /**
   * @return the type of message wrapped
   */
  public Type getType() {
    return mType;
  }

  /**
   * @return the serialized message as byte array
   */
  public byte[] toByteArray() {
    return mMessage.toByteArray();
  }

  /**
   * Parses a serialized bytes array into an instance denoted by type.
   *
   * @param type type of the class to parse to
   * @param serialized input byte array
   * @return instance as parsing result
   */
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
          throw new IllegalArgumentException("Unknown class type " + type.toString());
      }
      return new ProtoMessage(message, type);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
