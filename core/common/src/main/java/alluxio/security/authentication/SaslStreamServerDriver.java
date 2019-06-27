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
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.sasl.SaslException;

/**
 * Responsible for driving sasl traffic from server-side. Acts as a server's Sasl stream.
 */
public class SaslStreamServerDriver implements StreamObserver<SaslMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(SaslStreamServerDriver.class);

  /** Used to represent uninitialized channel Id. */
  private static final UUID EMPTY_UUID = new UUID(0L, 0L);
  /** Client's sasl stream. */
  private StreamObserver<SaslMessage> mRequestObserver = null;
  /** Handshake handler for server. */
  private SaslHandshakeServerHandler mSaslHandshakeServerHandler;
  /** Authentication server. */
  private AuthenticationServer mAuthenticationServer;
  /** Id for client-side channel that is authenticating. */
  private UUID mChannelId = EMPTY_UUID;
  /** Sasl server handler that will be used for authentication. */
  private SaslServerHandler mSaslServerHandler = null;
  /** Whether client stream observer is still valid. */
  private AtomicBoolean mClientStreamValid = new AtomicBoolean(true);

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
      if (mSaslHandshakeServerHandler == null) {
        // First message received from the client.
        // ChannelId and the AuthenticationName will be set only in the first call.
        LOG.debug("SaslServerDriver received authentication request of type:{} from channel: {}",
            saslMessage.getAuthenticationScheme(), saslMessage.getClientId());
        // Initialize this server driver accordingly.
        mChannelId = UUID.fromString(saslMessage.getClientId());
        // Get authentication server to create the Sasl handler for requested scheme.
        mSaslServerHandler =
            mAuthenticationServer.createSaslHandler(saslMessage.getAuthenticationScheme());
        mSaslHandshakeServerHandler = new DefaultSaslHandshakeServerHandler(mSaslServerHandler);
        // Unregister from registry if in case it was authenticated before.
        mAuthenticationServer.unregisterChannel(mChannelId);
      }

      LOG.debug("SaslServerDriver received message: {} from channel: {}", saslMessage, mChannelId);
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
      LOG.debug("Exception while handling SASL message: {} for channel: {}. Error: {}", saslMessage,
          mChannelId, se);
      mRequestObserver.onError(new UnauthenticatedException(se).toGrpcStatusException());
      closeHandler = true;
    } catch (UnauthenticatedException ue) {
      LOG.debug("Exception while handling SASL message: {} for channel: {}. Error: {}", saslMessage,
          mChannelId, ue);
      mRequestObserver.onError(ue.toGrpcStatusException());
      closeHandler = true;
    } catch (Exception e) {
      LOG.debug("Exception while handling SASL message: {} for channel: {}. Error: {}", saslMessage,
          mChannelId, e);
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
    LOG.warn("Error received for channel: {}. Error: {}", mChannelId, throwable);
    // Error on server invalidates client stream.
    mClientStreamValid.set(false);

    if (!mChannelId.equals(EMPTY_UUID)) {
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
    LOG.debug("Closing server driver for channel: {}", mChannelId);
    // Complete the client stream.
    completeStreamQuietly();
    // Close handler if not already.
    if (mSaslServerHandler != null) {
      try {
        mSaslServerHandler.close();
      } catch (Exception exc) {
        LogUtils.warnWithException(LOG, "Failed to close server driver for channel: {}.",
            mChannelId, exc);
      }
    }
  }

  /**
   * Completes the stream with a debug blanket over possible exceptions.
   */
  private void completeStreamQuietly() {
    if (mClientStreamValid.get() && mRequestObserver != null) {
      try {
        mRequestObserver.onCompleted();
      } catch (Exception exc) {
        LOG.debug("Failed to close authentication stream for channel: {}. Error: {}", mChannelId,
            exc);
      } finally {
        mClientStreamValid.set(false);
      }
    }
  }
}
