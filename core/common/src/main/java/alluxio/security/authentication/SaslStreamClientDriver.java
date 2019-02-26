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

import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.sasl.SaslException;

/**
 * Responsible for driving sasl traffic from client-side. Acts as a client's Sasl stream.
 */
public class SaslStreamClientDriver implements StreamObserver<SaslMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(SaslStreamClientDriver.class);
  /** Server's sasl stream. */
  private StreamObserver<SaslMessage> mRequestObserver;
  /** Handshake handler for client. */
  private SaslHandshakeClientHandler mSaslHandshakeClientHandler;
  /** Used to wait until authentication is completed. */
  private SettableFuture<Boolean> mAuthenticated;

  private final long mGrpcAuthTimeoutMs;

  /**
   * Creates client driver with given handshake handler.
   *
   * @param handshakeClient client handshake handler
   * @param grpcAuthTimeoutMs authentication timeout in milliseconds
   */
  public SaslStreamClientDriver(SaslHandshakeClientHandler handshakeClient,
      long grpcAuthTimeoutMs) {
    mSaslHandshakeClientHandler = handshakeClient;
    mAuthenticated = SettableFuture.create();
    mGrpcAuthTimeoutMs = grpcAuthTimeoutMs;
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
      LOG.debug("SaslClientDriver received message: {}",
          saslMessage != null ? saslMessage.getMessageType().toString() : "<NULL>");
      SaslMessage response = mSaslHandshakeClientHandler.handleSaslMessage(saslMessage);
      if (response == null) {
        mRequestObserver.onCompleted();
      } else {
        mRequestObserver.onNext(response);
      }
    } catch (SaslException e) {
      mAuthenticated.setException(e);
      mRequestObserver
          .onError(Status.fromCode(Status.Code.UNAUTHENTICATED).withCause(e).asException());
    }
  }

  @Override
  public void onError(Throwable throwable) {
    mAuthenticated.setException(throwable);
  }

  @Override
  public void onCompleted() {
    mAuthenticated.set(true);
  }

  /**
   * Starts authentication with the server and wait until completion.
   * @param channelId channel that is authenticating with the server
   * @throws UnauthenticatedException
   */
  public void start(String channelId) throws AlluxioStatusException {
    try {
      LOG.debug("Starting SASL handshake for ChannelId:{}", channelId);
      // Send the server initial message.
      mRequestObserver.onNext(mSaslHandshakeClientHandler.getInitialMessage(channelId));
      // Wait until authentication status changes.
      mAuthenticated.get(mGrpcAuthTimeoutMs, TimeUnit.MILLISECONDS);
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
}
