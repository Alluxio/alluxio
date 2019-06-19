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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.util.UUID;

/**
 * Default implementation of {@link SaslHandshakeClientHandler}.
 */
public class DefaultSaslHandshakeClientHandler implements SaslHandshakeClientHandler {
  /** Initial challenge for client to start Sasl session. */
  private static final byte[] S_INITIATE_CHALLENGE = new byte[0];

  /** SaslClientHandler that will be used be used for handshake. */
  private final SaslClientHandler mSaslClientHandler;

  /** SaslClient that is owned by given handler. */
  private final SaslClient mSaslClient;

  /**
   * Creates {@link DefaultSaslHandshakeClientHandler} with given {@link SaslClientHandler}.
   *
   * @param saslClientHandler sasl client handler
   */
  public DefaultSaslHandshakeClientHandler(SaslClientHandler saslClientHandler)
      throws UnauthenticatedException {
    mSaslClientHandler = saslClientHandler;
    mSaslClient = mSaslClientHandler.getSaslClient();
  }

  @Override
  public SaslMessage handleSaslMessage(SaslMessage message) throws SaslException {
    switch (message.getMessageType()) {
      case CHALLENGE:
        byte[] saslResponse = mSaslClient.evaluateChallenge(message.getMessage().toByteArray());
        SaslMessage.Builder response =
            SaslMessage.newBuilder().setMessageType(SaslMessageType.CHALLENGE);
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
  public SaslMessage getInitialMessage(UUID channelId) throws SaslException {
    byte[] initiateSaslResponse = null;
    if (mSaslClientHandler.getSaslClient().hasInitialResponse()) {
      initiateSaslResponse = mSaslClient.evaluateChallenge(S_INITIATE_CHALLENGE);
    }
    SaslMessage.Builder initialResponse =
        SaslMessage.newBuilder().setMessageType(SaslMessageType.CHALLENGE)
            .setAuthenticationScheme(mSaslClientHandler.getClientScheme());
    if (initiateSaslResponse != null) {
      initialResponse.setMessage(ByteString.copyFrom(initiateSaslResponse));
    }
    initialResponse.setClientId(channelId.toString());
    return initialResponse.build();
  }
}
