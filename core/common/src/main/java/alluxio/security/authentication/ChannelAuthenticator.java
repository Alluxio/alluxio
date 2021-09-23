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
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcChannelKey;
import alluxio.grpc.GrpcConnection;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.SaslAuthenticationServiceGrpc;
import alluxio.grpc.SaslMessage;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

import javax.security.auth.Subject;

/**
 * Used to authenticate with the target host. Used internally by {@link GrpcChannelBuilder}.
 */
public class ChannelAuthenticator {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelAuthenticator.class);

  /** Channel-key that is used to acquire the given managed channel. */
  private final GrpcChannelKey mChannelKey;
  /** The connection through which authentication will be established. */
  private final GrpcConnection mConnection;
  /** Subject of authentication. */
  private final Subject mParentSubject;
  /** Requested auth type. */
  private final AuthType mAuthType;

  /** Alluxio client configuration. */
  private AlluxioConfiguration mConfiguration;

  /** Client-side authentication driver. */
  private AuthenticatedChannelClientDriver mAuthDriver;

  /** Authenticated logical channel. */
  private Channel mAuthenticatedChannel;

  /**
   * Creates {@link ChannelAuthenticator} instance.
   *
   * @param connection the gRPC connection
   * @param subject the javax subject to use for authentication
   * @param authType the requested authentication type
   * @param conf the Alluxio configuration
   */
  public ChannelAuthenticator(GrpcConnection connection, Subject subject, AuthType authType,
      AlluxioConfiguration conf) {
    mConnection = connection;
    mChannelKey = mConnection.getChannelKey();
    mParentSubject = subject;
    mAuthType = authType;
    mConfiguration = conf;
  }

  /**
   * It builds an authenticated channel.
   *
   * @throws AlluxioStatusException
   */
  public void authenticate() throws AlluxioStatusException {
    LOG.debug("Authenticating channel: {}. AuthType: {}", mChannelKey.toStringShort(), mAuthType);

    ChannelAuthenticationScheme authScheme = getChannelAuthScheme(mAuthType, mParentSubject,
        mChannelKey.getServerAddress().getSocketAddress());

    try {
      // Create client-side driver for establishing authenticated channel with the target.
      mAuthDriver = new AuthenticatedChannelClientDriver(
          createSaslClientHandler(mChannelKey.getServerAddress(), authScheme, mParentSubject),
          mChannelKey);

      // Initialize client-server authentication drivers.
      SaslAuthenticationServiceGrpc.SaslAuthenticationServiceStub serverStub =
          SaslAuthenticationServiceGrpc.newStub(mConnection.getChannel());

      StreamObserver<SaslMessage> requestObserver = serverStub.authenticate(mAuthDriver);
      mAuthDriver.setServerObserver(requestObserver);

      // Start authentication with the target. (This is blocking.)
      long authTimeout = mConfiguration.getMs(PropertyKey.NETWORK_CONNECTION_AUTH_TIMEOUT);
      mAuthDriver.startAuthenticatedChannel(authTimeout);

      // Intercept authenticated channel with channel-id injector.
      mConnection.interceptChannel(new ChannelIdInjector(mChannelKey.getChannelId()));
    } catch (Throwable t) {
      AlluxioStatusException e = AlluxioStatusException.fromThrowable(t);
      // Build a pretty message for authentication failure.
      String message = String.format(
          "Channel authentication failed with code:%s. Channel: %s, AuthType: %s, Error: %s",
          e.getStatusCode().name(), mChannelKey.toStringShort(), mAuthType, e.toString());
      throw AlluxioStatusException
          .from(Status.fromCode(e.getStatusCode()).withDescription(message).withCause(t));
    }
  }

  /**
   * @return the authenticated {@link Channel} instance
   */
  public Channel getAuthenticatedChannel() {
    return mAuthenticatedChannel;
  }

  /**
   * @return the authentication driver
   */
  public AuthenticatedChannelClientDriver getAuthenticationDriver() {
    return mAuthDriver;
  }

  /**
   * Determines transport level authentication scheme for given subject.
   *
   * @param subject the subject
   * @param serverAddress the target server address
   * @return the channel authentication scheme to use
   * @throws UnauthenticatedException if configured authentication type is not supported
   */
  private ChannelAuthenticationScheme getChannelAuthScheme(AuthType authType, Subject subject,
      SocketAddress serverAddress) throws UnauthenticatedException {
    switch (authType) {
      case NOSASL:
        return ChannelAuthenticationScheme.NOSASL;
      case SIMPLE:
        return ChannelAuthenticationScheme.SIMPLE;
      case CUSTOM:
        return ChannelAuthenticationScheme.CUSTOM;
      default:
        throw new UnauthenticatedException(String.format(
            "Configured authentication type is not supported: %s", authType.getAuthName()));
    }
  }

  /**
   * Create Sasl level handler for client.
   *
   * @param serverAddress target server address
   * @param authScheme authentication scheme to use
   * @param subject the subject to use
   * @return the created {@link SaslClientHandler} instance
   * @throws UnauthenticatedException
   */
  private SaslClientHandler createSaslClientHandler(GrpcServerAddress serverAddress,
      ChannelAuthenticationScheme authScheme, Subject subject) throws UnauthenticatedException {
    switch (authScheme) {
      case SIMPLE:
      case CUSTOM:
        return new alluxio.security.authentication.plain.SaslClientHandlerPlain(mParentSubject,
            mConfiguration);
      default:
        throw new UnauthenticatedException(
            String.format("Channel authentication scheme not supported: %s", authScheme.name()));
    }
  }
}
