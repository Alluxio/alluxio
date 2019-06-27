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
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.exception.status.UnknownException;
import alluxio.grpc.SaslMessage;
import alluxio.util.LogUtils;

import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.sasl.SaslException;

/**
 * Responsible for driving sasl traffic from client-side. Acts as a client's Sasl stream.
 *
 * A Sasl authentication between client and server is managed by {@link SaslStreamClientDriver} and
 * {@link SaslStreamServerDriver} respectively. This drivers are wrappers over gRPC
 * {@link StreamObserver}s that manages the stream traffic destined for the other participant. They
 * make sure messages are exchanged between client and server one by one synchronously.
 *
 * Sasl handshake is initiated by the client. Following the initiate call, depending on the scheme,
 * one or more messages are exchanged to establish authenticated session between client and server.
 * After the authentication is established, client and server streams are not closed in order to use
 * them as long polling on authentication state changes. Client closing the stream means that it
 * doesn't want to be authenticated anymore. Server closing the stream means the client is not
 * authenticated at the server anymore.
 *
 */
public class SaslStreamClientDriver implements StreamObserver<SaslMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(SaslStreamClientDriver.class);
  /** Server's sasl stream. */
  private StreamObserver<SaslMessage> mRequestObserver;
  /** Handshake handler for client. */
  private SaslHandshakeClientHandler mSaslHandshakeClientHandler;
  /** Used to wait during authentication handshake. */
  private SettableFuture<Boolean> mHandshakeFuture;
  /** Current authenticated state. */
  private AtomicBoolean mAuthenticated;
  /** Channel Id that has started the authentication. */
  private UUID mChannelId;

  private final long mGrpcAuthTimeoutMs;

  /**
   * Creates client driver with given handshake handler.
   *
   * @param handshakeClient client handshake handler
   * @param authenticated boolean reference to receive authentication state changes
   * @param channelId channel Id for authentication
   * @param grpcAuthTimeoutMs authentication timeout in milliseconds
   */
  public SaslStreamClientDriver(SaslHandshakeClientHandler handshakeClient,
      AtomicBoolean authenticated, UUID channelId, long grpcAuthTimeoutMs) {
    mSaslHandshakeClientHandler = handshakeClient;
    mHandshakeFuture = SettableFuture.create();
    mChannelId = channelId;
    mGrpcAuthTimeoutMs = grpcAuthTimeoutMs;
    mAuthenticated = authenticated;
  }

  /**
   * Sets the server's Sasl stream.
   *
   * @param requestObserver server Sasl stream
   */
  public void setServerObserver(StreamObserver<SaslMessage> requestObserver) {
    mRequestObserver = requestObserver;
  }

  @Override
  public void onNext(SaslMessage saslMessage) {
    try {
      LOG.debug("SaslClientDriver received message: {} for channel: {}", saslMessage, mChannelId);
      SaslMessage response = mSaslHandshakeClientHandler.handleSaslMessage(saslMessage);
      if (response != null) {
        mRequestObserver.onNext(response);
      } else {
        // {@code null} response means server message was a success.
        mHandshakeFuture.set(true);
      }
    } catch (Exception e) {
      LOG.debug("Exception while handling SASL message: {} for channel: {}. Error: {}", saslMessage,
          mChannelId, e);
      mHandshakeFuture.setException(e);
      mRequestObserver.onError(e);
    }
  }

  @Override
  public void onError(Throwable throwable) {
    LOG.warn("Received error on client driver for channel: {}. Error: {}", mChannelId, throwable);
    mHandshakeFuture.setException(throwable);
  }

  @Override
  public void onCompleted() {
    LOG.debug("Client authentication closed by server for channel: {}", mChannelId);
    // Server completes the stream when authenticated session is terminated/revoked.
    mAuthenticated.set(false);
  }

  /**
   * Starts authentication with the server and wait until completion.
   * @throws UnauthenticatedException
   */
  public void start() throws AlluxioStatusException {
    try {
      LOG.debug("Starting SASL handshake for channel: {}", mChannelId);
      // Send the server initial message.
      mRequestObserver.onNext(mSaslHandshakeClientHandler.getInitialMessage(mChannelId));
      // Wait until authentication status changes.
      mAuthenticated.set(mHandshakeFuture.get(mGrpcAuthTimeoutMs, TimeUnit.MILLISECONDS));
    } catch (SaslException se) {
      throw new UnauthenticatedException(se.getMessage(), se);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new UnavailableException(ie.getMessage(), ie);
    } catch (ExecutionException e) {
      Throwable cause = (e.getCause() != null) ? e.getCause() : e;
      if (cause != null && cause instanceof StatusRuntimeException) {
        StatusRuntimeException sre = (StatusRuntimeException) cause;
        // If caught unimplemented, that means server does not support authentication.
        if (sre.getStatus().getCode() == Status.Code.UNIMPLEMENTED) {
          throw new UnauthenticatedException("Authentication is disabled on target host.");
        }
        throw AlluxioStatusException.fromStatusRuntimeException((StatusRuntimeException) cause);
      }
      throw new UnknownException(cause.getMessage(), cause);
    } catch (TimeoutException e) {
      throw new UnavailableException(e);
    }
  }

  /**
   * Stops authenticated session with the server by releasing the long poll.
   */
  public void stop() {
    LOG.debug("Closing client driver for channel: {}", mChannelId);
    try {
      if (mAuthenticated.get()) {
        mRequestObserver.onCompleted();
      }
    } catch (Exception exc) {
      LogUtils.warnWithException(LOG, "Failed stopping authentication session with server.", exc);
    }
  }
}
