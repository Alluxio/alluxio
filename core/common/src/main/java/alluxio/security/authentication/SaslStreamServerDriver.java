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

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.SaslMessage;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Responsible for driving sasl traffic from server-side. Acts as a server's Sasl stream.
 */
public class SaslStreamServerDriver implements StreamObserver<SaslMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(SaslStreamServerDriver.class);
  /** Client's sasl stream. */
  private StreamObserver<SaslMessage> mRequestObserver = null;
  /** Handshake handler for server. */
  private SaslHandshakeServerHandler mSaslHandshakeServerHandler;
  /** Authentication server. */
  private AuthenticationServer mAuthenticationServer;
  /** Id for client-side channel that is authenticating. */
  private UUID mChannelId;
  /** Sasl server that will be used for authentication. */
  private SaslServer mSaslServer = null;
  private final AlluxioConfiguration mConfiguration;

  /**
   * Creates {@link SaslStreamServerDriver} for given {@link AuthenticationServer}.
   *
   * @param authenticationServer authentication server
   * @param conf Alluxio configuration
   */
  public SaslStreamServerDriver(AuthenticationServer authenticationServer,
      AlluxioConfiguration conf) {
    mAuthenticationServer = authenticationServer;
    mConfiguration = conf;
  }

  /**
   * Sets the client's Sasl stream.
   *
   * @param requestObserver client Sasl stream
   */
  public void setClientObserver(StreamObserver<SaslMessage> requestObserver) {
    mRequestObserver = requestObserver;
  }

  @Override
  public void onNext(SaslMessage saslMessage) {
    try {
      LOG.debug("SaslServerDriver received message: {}",
          saslMessage != null ? saslMessage.getMessageType().toString() : "<NULL>");

      if (mSaslHandshakeServerHandler == null) {
        // First message received from the client.
        // ChannelId and the AuthenticationName will be set only in the first call.
        // Initialize this server driver accordingly.
        mChannelId = UUID.fromString(saslMessage.getClientId());
        AuthType authType = AuthType.valueOf(saslMessage.getAuthenticationName());
        LOG.debug("SaslServerDriver received authentication request. ChannelId: {}, AuthType: {}",
            mChannelId, authType);
        // TODO(ggezer) wire server name?
        mSaslServer =
            SaslParticipantProvider.Factory.create(authType).createSaslServer("localhost",
                mConfiguration);
        mSaslHandshakeServerHandler =
            SaslHandshakeServerHandler.Factory.create(authType, mSaslServer);
        // Unregister from registry if in case it was authenticated before.
        mAuthenticationServer.unregisterChannel(mChannelId);
      }
      // Respond to client.
      mRequestObserver.onNext(mSaslHandshakeServerHandler.handleSaslMessage(saslMessage));
    } catch (SaslException se) {
      mRequestObserver.onError(new UnauthenticatedException(se).toGrpcStatusException());
    } catch (UnauthenticatedException ue) {
      mRequestObserver.onError(ue.toGrpcStatusException());
    }
  }

  @Override
  public void onError(Throwable throwable) {}

  @Override
  public void onCompleted() {
    mAuthenticationServer.registerChannel(mChannelId, mSaslServer.getAuthorizationID(),
        mSaslServer);
    mRequestObserver.onCompleted();
  }
}
