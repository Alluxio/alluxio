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

import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.grpc.SaslMessage;
import alluxio.grpc.SaslMessageType;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * Abstract {@link SaslClientHandler} implementation that maintains {@link SaslClient} instance.
 */
public abstract class AbstractSaslClientHandler implements SaslClientHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSaslClientHandler.class);

  /** Initial challenge for client to start Sasl session. */
  private static final byte[] S_INITIATE_CHALLENGE = new byte[0];

  /** The scheme for SaslClient.  */
  private final ChannelAuthenticationScheme mAuthScheme;
  /** Underlying SaslClient. */
  protected SaslClient mSaslClient;

  /**
   * Creates an abstract sasl handler for given scheme.
   *
   * @param authScheme channel authentication scheme
   */
  public AbstractSaslClientHandler(ChannelAuthenticationScheme authScheme) {
    mAuthScheme = authScheme;
  }

  /**
   * This is synchronized in order to protect {@link #mSaslClient}.
   */
  @Override
  public synchronized SaslMessage handleMessage(SaslMessage message) throws SaslException {
    if (mSaslClient == null) {
      throw new SaslException("SaslClient handler is closed");
    }
    // Generate initial message.
    if (message == null) {
      SaslMessage.Builder initialResponse = SaslMessage.newBuilder()
          .setMessageType(SaslMessageType.CHALLENGE)
          .setAuthenticationScheme(mAuthScheme);

      if (mSaslClient.hasInitialResponse()) {
        byte[] initiateSaslResponse = mSaslClient.evaluateChallenge(S_INITIATE_CHALLENGE);
        if (initiateSaslResponse != null) {
          initialResponse.setMessage(ByteString.copyFrom(initiateSaslResponse));
        }
      }
      return initialResponse.build();
    }
    // Respond to a sasl message.
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

  /**
   * This is synchronized in order to protect {@link #mSaslClient}.
   */
  @Override
  public synchronized void close() {
    if (mSaslClient != null) {
      try {
        mSaslClient.dispose();
      } catch (SaslException exc) {
        LOG.debug("Failed to close SaslClient.", exc);
      } finally {
        mSaslClient = null;
      }
    }
  }
}
