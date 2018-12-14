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
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.SaslHandshakeClientHandler;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * Implementation of {@link SaslHandshakeClientHandler} for plain authentication.
 */
public class SaslHandshakeClientHandlerPlain implements SaslHandshakeClientHandler {

  /** SaslClient that will be used. */
  private SaslClient mSaslClient;

  /**
   * Creates {@link SaslHandshakeClientHandlerPlain} with given {@link SaslClient}.
   *
   * @param saslClient sasl client
   */
  public SaslHandshakeClientHandlerPlain(SaslClient saslClient) {
    mSaslClient = saslClient;
  }

  @Override
  public SaslMessage handleSaslMessage(SaslMessage message) throws SaslException {
    switch (message.getMessageType()) {
      case CHALLANGE:
        byte[] saslResponse = mSaslClient.evaluateChallenge(message.getMessage().toByteArray());
        SaslMessage.Builder response =
            SaslMessage.newBuilder().setMessageType(SaslMessageType.CHALLANGE);
        if (saslResponse != null) {
          response.setMessage(ByteString.copyFrom(saslResponse));
        }
        return response.build();
      case SUCCESS:
        if (message.hasMessage()) {
          mSaslClient.evaluateChallenge(message.getMessage().toByteArray());
        }
        Preconditions.checkArgument(mSaslClient.isComplete());
        return null;
      default:
        throw new SaslException(
            "Client can't process Sasl message type:" + message.getMessageType().name());
    }
  }

  @Override
  public SaslMessage getInitialMessage(String channelId) throws SaslException {
    byte[] initiateSaslResponse = null;
    if (mSaslClient.hasInitialResponse()) {
      initiateSaslResponse = mSaslClient.evaluateChallenge(new byte[0]);
    }
    SaslMessage.Builder initialResponse =
        SaslMessage.newBuilder().setMessageType(SaslMessageType.CHALLANGE)
            .setAuthenticationName(AuthType.SIMPLE.getAuthName());
    if (initiateSaslResponse != null) {
      initialResponse.setMessage(ByteString.copyFrom(initiateSaslResponse));
    }
    initialResponse.setClientId(channelId);
    return initialResponse.build();
  }
}
