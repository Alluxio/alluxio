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

package alluxio.security.authentication.plain;

import alluxio.grpc.SaslMessage;
import alluxio.grpc.SaslMessageType;
import alluxio.security.authentication.SaslHandshakeServerHandler;

import com.google.protobuf.ByteString;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Implementation of {@link SaslHandshakeServerHandler} for plain authentication.
 */
public class SaslHandshakeServerHandlerPlain implements SaslHandshakeServerHandler {

  /** SaslServer that will be used. */
  private final SaslServer mSaslServer;

  /**
   * Creates {@link SaslHandshakeServerHandlerPlain} with given {@link SaslServer}.
   *
   * @param saslServer sasl server
   */
  public SaslHandshakeServerHandlerPlain(SaslServer saslServer) {
    mSaslServer = saslServer;
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
