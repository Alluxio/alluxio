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

import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.grpc.SaslMessage;
import alluxio.grpc.SaslMessageType;

import com.google.common.base.MoreObjects;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import javax.security.sasl.SaslException;

/**
 * Responsible for driving authentication traffic from server-side.
 */
public class AuthenticatedChannelServerDriver implements StreamObserver<SaslMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticatedChannelServerDriver.class);

  /** Used to represent uninitialized channel Id. */
  private static final UUID EMPTY_UUID = new UUID(0L, 0L);

  /** Client's sasl stream. */
  private StreamObserver<SaslMessage> mRequestObserver = null;
  /** Authentication server. */
  private AuthenticationServer mAuthenticationServer;

  /** Id for client-side channel that is authenticating. */
  private UUID mChannelId = EMPTY_UUID;
  /** Reference for client owning the channel. */
  private String mChannelRefStr;
  /** Sasl server handler that will be used to build secure stream. */
  private SaslServerHandler mSaslServerHandler = null;
  /** Whether channel is authenticated. */
  private volatile boolean mChannelAuthenticated = false;

  /**
   * Creates {@link AuthenticatedChannelServerDriver} for given {@link AuthenticationServer}.
   *
   * @param authenticationServer authentication server
   */
  public AuthenticatedChannelServerDriver(AuthenticationServer authenticationServer) {
    mAuthenticationServer = authenticationServer;
  }

  /**
   * Sets the client's Sasl stream.
   *
   * @param requestObserver client Sasl stream
   */
  public void setClientObserver(StreamObserver<SaslMessage> requestObserver) {
    mRequestObserver = requestObserver;
  }

  private void initAuthenticatedChannel(ChannelAuthenticationScheme authScheme, UUID channelId,
                                        String channelOwner) throws SaslException {
    LOG.debug("Initializing authentication for {}. AuthType: {}", mChannelRefStr, authScheme);
    // Create sasl handler for the requested scheme.
    mSaslServerHandler = mAuthenticationServer.createSaslHandler(authScheme);
    // Unregister from registry if in case it was authenticated before.
    mAuthenticationServer.unregisterChannel(mChannelId);
    // Initialize channel id
    mChannelId = channelId;
    // Build channel reference for further tracing.
    mChannelRefStr = MoreObjects.toStringHelper("Channel")
        .add("ChannelId", channelId.toString())
        .add("Owner", channelOwner).toString();
  }

  private void channelAuthenticated(AuthenticatedUserInfo authUserInfo) {
    // Register authorized user with the authentication server.
    mAuthenticationServer.registerChannel(mChannelId, authUserInfo, this);
    mChannelAuthenticated = true;
  }

  private void closeAuthenticatedChannel(boolean signalOwner) {
    if (mChannelAuthenticated) {
      LOG.debug("Closing authenticated channel for {}", mChannelRefStr);
      mAuthenticationServer.unregisterChannel(mChannelId);
      mChannelAuthenticated = false;
    }

    if (mSaslServerHandler != null) {
      mSaslServerHandler.close();
    }

    if (signalOwner) {
      try {
        // Complete stream.
        mRequestObserver.onCompleted();
      } catch (Exception exc) {
        LOG.debug("Failed to close gRPC stream of {}. Error: {}", mChannelRefStr, exc);
      }
    }
  }

  @Override
  public void onNext(SaslMessage saslMessage) {
    try {
      if (mChannelId.equals(EMPTY_UUID)) {
        initAuthenticatedChannel(
            saslMessage.getAuthenticationScheme(),
            UUID.fromString(saslMessage.getClientId()),
            saslMessage.getChannelOwner());
      }

      LOG.debug("Responding to a message of {}. Message: {}", mChannelRefStr, saslMessage);
      // Consult sasl server for handling the message.
      SaslMessage response = mSaslServerHandler.handleMessage(saslMessage);

      // Activate if sasl is secured.
      if (response.getMessageType() == SaslMessageType.SUCCESS) {
        channelAuthenticated(mSaslServerHandler.getAuthenticatedUserInfo());
      }
      // Push response to stream.
      mRequestObserver.onNext(response);
    } catch (Throwable t) {
      LOG.debug("Exception while handling message of {}. Message: {}. Error: {}",
              mChannelRefStr, saslMessage, t);
      // Invalidate stream.
      mRequestObserver.onError(AlluxioStatusException.fromThrowable(t).toGrpcStatusException());
      closeAuthenticatedChannel(false);
    }
  }

  @Override
  public void onError(Throwable throwable) {
    closeAuthenticatedChannel(false);
  }

  @Override
  public void onCompleted() {
    closeAuthenticatedChannel(true);
  }

  /**
   * Completes authenticated channel.
   */
  public void close() {
    LOG.debug("Closing authentication for {}", mChannelRefStr);
    closeAuthenticatedChannel(true);
  }
}
