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
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.SaslMessage;

import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.security.sasl.SaslException;
import java.util.concurrent.ExecutionException;

/**
 * Responsible for driving sasl traffic from client-side. Acts as a client's Sasl stream.
 */
public class SaslStreamClientDriver implements StreamObserver<SaslMessage> {
  /** Server's sasl stream. */
  private StreamObserver<SaslMessage> mRequestObserver;
  /** Handshake handler for client. */
  private SaslHandshakeClientHandler mSaslHandshakeClientHandler;
  /** Used to wait until authentication is completed. */
  private SettableFuture<Boolean> mAuthenticated;

  /**
   * Creates client driver with given handshake handler.
   *
   * @param handshakeClient client handshake handler
   */
  public SaslStreamClientDriver(SaslHandshakeClientHandler handshakeClient) {
    mSaslHandshakeClientHandler = handshakeClient;
    mAuthenticated = SettableFuture.create();
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
  public void start(String channelId) throws UnauthenticatedException, UnavailableException {
    try {
      // Send the server initial message.
      mRequestObserver.onNext(mSaslHandshakeClientHandler.getInitialMessage(channelId));
      // Wait until authentication status changes.
      mAuthenticated.get();
    } catch (SaslException se) {
      throw new UnauthenticatedException(se.getMessage(), se);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new UnauthenticatedException(ie.getMessage(), ie);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause != null && cause instanceof StatusRuntimeException) {
        if (((StatusRuntimeException) cause).getStatus().getCode() == Status.Code.UNAVAILABLE) {
          throw new UnavailableException(cause.getMessage(), cause);
        }
      }
      throw new UnauthenticatedException(cause.getMessage(), cause);
    }
  }
}
