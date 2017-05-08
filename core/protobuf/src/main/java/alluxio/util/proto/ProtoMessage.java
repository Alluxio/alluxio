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

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

/**
 * A simple wrapper around the MessageLite class in Protobuf for a few messages defined and
 * generated in Alluxio. In other parts of Alluxio code base that are outside of this module,
 * use this class to replace MessageLite when it must reference MessageLite as a base class of
 * different generated messages. This class is intended to be used internally only.
 */
public final class ProtoMessage {
  private MessageLite mMessage;

  /**
   * Constructs a {@link ProtoMessage} instance wrapping around {@link MessageLite}.
   *
   * @param message the message to wrap
   */
  private ProtoMessage(MessageLite message) {
    mMessage = message;
  }

  /**
   * @param readRequest the read request
   */
  public ProtoMessage(Protocol.ReadRequest readRequest) {
    mMessage = readRequest;
  }

  /**
   * @param writeRequest the write request
   */
  public ProtoMessage(Protocol.WriteRequest writeRequest) {
    mMessage = writeRequest;
  }

  /**
   * @param response  the response
   */
  public ProtoMessage(Protocol.Response response) {
    mMessage = response;
  }

  /**
   * @param request the local block open request
   */
  public ProtoMessage(Protocol.LocalBlockOpenRequest request) {
    mMessage = request;
  }

  /**
   * @param response the local block open response
   */
  public ProtoMessage(Protocol.LocalBlockOpenResponse response) {
    mMessage = response;
  }

  /**
   * @param request the local block close request
   */
  public ProtoMessage(Protocol.LocalBlockCloseRequest request) {
    mMessage = request;
  }

  /**
   * Gets the read request or throws runtime exception if mMessage is not of type
   * {@link Protocol.ReadRequest}.
   *
   * @return the read request
   */
  public Protocol.ReadRequest asReadRequest() {
    Preconditions.checkState(mMessage instanceof Protocol.ReadRequest);
    return (Protocol.ReadRequest) mMessage;
  }

  /**
   * @return true if mMessage is of type {@link Protocol.ReadRequest}
   */
  public boolean isReadRequest() {
    return mMessage instanceof Protocol.ReadRequest;
  }

  /**
   * Gets the write request or throws runtime exception if mMessage is not of type
   * {@link Protocol.WriteRequest}.
   *
   * @return the write request
   */
  public Protocol.WriteRequest asWriteRequest() {
    Preconditions.checkState(mMessage instanceof Protocol.WriteRequest);
    return (Protocol.WriteRequest) mMessage;
  }

  /**
   * @return true if mMessage is of type {@link Protocol.WriteRequest}
   */
  public boolean isWriteRequest() {
    return mMessage instanceof Protocol.WriteRequest;
  }

  /**
   * Gets the response or throws runtime exception if mMessage is not of type
   * {@link Protocol.Response}.
   *
   * @return the response
   */
  public Protocol.Response asResponse() {
    Preconditions.checkState(mMessage instanceof Protocol.Response);
    return (Protocol.Response) mMessage;
  }

  /**
   * @return true if mMessage is of type {@link Protocol.Response}
   */
  public boolean isResponse() {
    return mMessage instanceof Protocol.Response;
  }

  /**
   * Gets the local block open request or throws runtime exception if mMessage is not of type
   * {@link Protocol.LocalBlockOpenRequest}.
   *
   * @return the Protocol.LocalBlockOpenRequest
   */
  public Protocol.LocalBlockOpenRequest asLocalBlockOpenRequest() {
    Preconditions.checkState(mMessage instanceof Protocol.LocalBlockOpenRequest);
    return (Protocol.LocalBlockOpenRequest) mMessage;
  }

  /**
   * @return true if mMessage is of type {@link Protocol.LocalBlockOpenRequest}
   */
  public boolean isLocalBlockOpenRequest() {
    return mMessage instanceof Protocol.LocalBlockOpenRequest;
  }

  /**
   * Gets the local block open response or throws runtime exception if mMessage is not of type
   * {@link Protocol.LocalBlockOpenResponse}.
   *
   * @return the Protocol.LocalBlockOpenResponse
   */
  public Protocol.LocalBlockOpenResponse asLocalBlockOpenResponse() {
    Preconditions.checkState(mMessage instanceof Protocol.LocalBlockOpenResponse);
    return (Protocol.LocalBlockOpenResponse) mMessage;
  }

  /**
   * @return true if mMessage is of type {@link Protocol.LocalBlockOpenResponse}
   */
  public boolean isLocalBlockOpenResponse() {
    return mMessage instanceof Protocol.LocalBlockOpenResponse;
  }

  /**
   * Gets the local block close request or throws runtime exception if mMessage is not of type
   * {@link Protocol.LocalBlockCloseRequest}.
   *
   * @return the Protocol.LocalBlockCloseRequest
   */
  public Protocol.LocalBlockCloseRequest asLocalBlockCloseRequest() {
    Preconditions.checkState(mMessage instanceof Protocol.LocalBlockCloseRequest);
    return (Protocol.LocalBlockCloseRequest) mMessage;
  }

  /**
   * @return true if mMessage is of type {@link Protocol.LocalBlockCloseRequest}
   */
  public boolean isLocalBlockCloseRequest() {
    return mMessage instanceof Protocol.LocalBlockCloseRequest;
  }

  /**
   * @return the serialized message as byte array
   */
  public byte[] toByteArray() {
    return mMessage.toByteArray();
  }

  /**
   * Parses proto message from bytes given a prototype.
   *
   * @param serialized the serialized message
   * @param prototype the prototype of the message to return which is usually constructed via
   *        new ProtoMessage(SomeProtoType.getDefaultInstance())
   * @return the proto message
   */
  public static ProtoMessage parseFrom(byte[] serialized, ProtoMessage prototype) {
    try {
      return new ProtoMessage(prototype.mMessage.getParserForType().parseFrom(serialized));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
