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

package alluxio.security.authentication;

import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.SaslMessage;
import alluxio.grpc.SaslMessageType;

import com.google.protobuf.ByteString;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Default implementation of {@link DefaultSaslHandshakeServerHandler}.
 */
public class DefaultSaslHandshakeServerHandler implements SaslHandshakeServerHandler {
  /** SaslClientHandler that will be used be used for handshake. */
  private final SaslServerHandler mSaslServerHandler;

  /** SaslClient that is owned by given handler. */
  private final SaslServer mSaslServer;

  /**
   * Creates {@link DefaultSaslHandshakeServerHandler} with given {@link SaslServerHandler}.
   *
   * @param saslServerHandler sasl server handler
   */
  public DefaultSaslHandshakeServerHandler(SaslServerHandler saslServerHandler)
      throws UnauthenticatedException {
    mSaslServerHandler = saslServerHandler;
    mSaslServer = mSaslServerHandler.getSaslServer();
  }

  @Override
  public SaslMessage handleSaslMessage(SaslMessage message) throws SaslException {
    switch (message.getMessageType()) {
      case CHALLENGE:
        byte[] saslResponse = mSaslServer.evaluateResponse(message.getMessage().toByteArray());
        SaslMessage.Builder response = SaslMessage.newBuilder();
        if (mSaslServer.isComplete()) {
          response.setMessageType(SaslMessageType.SUCCESS);
        } else {
          response.setMessageType(SaslMessageType.CHALLENGE);
        }
        if (saslResponse != null) {
          response.setMessage(ByteString.copyFrom(saslResponse));
        }
        return response.build();
      default:
        throw new SaslException(
            "Server can't process Sasl message type:" + message.getMessageType().name());
    }
  }
}
