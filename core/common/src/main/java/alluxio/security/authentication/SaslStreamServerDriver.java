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
import alluxio.util.LogUtils;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import javax.security.sasl.SaslException;

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
  /** Sasl server handler that will be used for authentication. */
  private SaslServerHandler mSaslServerHandler = null;

  /**
   * Creates {@link SaslStreamServerDriver} for given {@link AuthenticationServer}.
   *
   * @param authenticationServer authentication server
   */
  public SaslStreamServerDriver(AuthenticationServer authenticationServer) {
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

  @Override
  public void onNext(SaslMessage saslMessage) {
    /** Whether to close the handler after this message.  */
    boolean closeHandler = false;
    try {
      LOG.debug("SaslServerDriver received message: {}",
          saslMessage != null ? saslMessage.getMessageType().toString() : "<NULL>");

      if (mSaslHandshakeServerHandler == null) {
        // First message received from the client.
        // ChannelId and the AuthenticationName will be set only in the first call.
        // Initialize this server driver accordingly.
        mChannelId = UUID.fromString(saslMessage.getClientId());
        // Get authentication server to create the Sasl handler for requested scheme.
        mSaslServerHandler =
            mAuthenticationServer.createSaslHandler(saslMessage.getAuthenticationScheme());
        mSaslHandshakeServerHandler = new DefaultSaslHandshakeServerHandler(mSaslServerHandler);
        // Unregister from registry if in case it was authenticated before.
        mAuthenticationServer.unregisterChannel(mChannelId);
      }
      // Respond to client.
      SaslMessage response = mSaslHandshakeServerHandler.handleSaslMessage(saslMessage);
      // Complete the call from server-side before sending success response to client.
      // Because client will assume authenticated after receiving the success message.
      if (response.getMessageType() == SaslMessageType.SUCCESS) {
        // Register authorized user with the authentication server.
        mAuthenticationServer.registerChannel(mChannelId,
            mSaslServerHandler.getAuthenticatedUserInfo(), this);
        // Finished with the handler.
        closeHandler = true;
      }
      mRequestObserver.onNext(response);
    } catch (SaslException se) {
      LOG.debug("Exception while handling SASL message: {}", saslMessage, se);
      mRequestObserver.onError(new UnauthenticatedException(se).toGrpcStatusException());
      closeHandler = true;
    } catch (UnauthenticatedException ue) {
      LOG.debug("Exception while handling SASL message: {}", saslMessage, ue);
      mRequestObserver.onError(ue.toGrpcStatusException());
      closeHandler = true;
    } catch (Exception e) {
      LOG.debug("Exception while handling SASL message: {}", saslMessage, e);
      closeHandler = true;
      throw e;
    } finally {
      if (closeHandler) {
        try {
          mSaslServerHandler.close();
        } catch (IOException exc) {
          LOG.debug("Failed to close SaslServer.", exc);
        }
      }
    }
  }

  @Override
  public void onError(Throwable throwable) {
    if (mChannelId != null) {
      LOG.debug("Closing authenticated channel: {} due to error: {}", mChannelId, throwable);
      if (!mAuthenticationServer.unregisterChannel(mChannelId)) {
        // Channel was not registered. Close driver explicitly.
        close();
      }
    }
  }

  @Override
  public void onCompleted() {
    // Client completes the stream when authenticated channel is being closed.
    LOG.debug("Received completion for authenticated channel: {}", mChannelId);
    // close() will be called by unregister channel if it was registered.
    if (!mAuthenticationServer.unregisterChannel(mChannelId)) {
      // Channel was not registered. Close driver explicitly.
      close();
    }
  }

  /**
   * Closes the authentication stream.
   */
  public void close() {
    // Complete the client stream.
    completeStreamQuietly();
    // Close handler if not already.
    if (mSaslServerHandler != null) {
      try {
        mSaslServerHandler.close();
      } catch (Exception exc) {
        LogUtils.warnWithException(LOG, "Failed to close server driver for channel: {}.",
            (mChannelId != null) ? mChannelId : "<NULL>", exc);
      }
    }
  }

  private void completeStreamQuietly() {
    if (mRequestObserver != null) {
      try {
        mRequestObserver.onCompleted();
      } catch (Exception exc) {
        LOG.debug("Failed to close authentication stream from server.", exc);
      }
    }
  }
}
