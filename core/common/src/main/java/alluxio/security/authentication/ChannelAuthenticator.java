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
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.SaslAuthenticationServiceGrpc;
import alluxio.grpc.SaslMessage;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;

/**
 * Used to authenticate with the target host. Used internally by {@link GrpcChannelBuilder}.
 */
public class ChannelAuthenticator {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelAuthenticator.class);

  /** Alluxio client configuration. */
  private AlluxioConfiguration mConfiguration;

  /** Whether to use mParentSubject as authentication user. */
  protected boolean mUseSubject;
  /** Subject for authentication. */
  protected Subject mParentSubject;

  /* Used in place of a subject. */
  protected String mUserName;
  protected String mPassword;
  protected String mImpersonationUser;

  /** Authentication type to use with the target host. */
  protected AuthType mAuthType;

  /** gRPC Authentication timeout in milliseconds. */
  protected final long mGrpcAuthTimeoutMs;

  /** Key for to-be-authenticated channel. */
  protected final GrpcChannelKey mChannelKey;

  /**
   * Creates {@link ChannelAuthenticator} instance.
   *
   * @param channelKey channel key
   * @param subject javax subject to use for authentication
   * @param conf Alluxio configuration
   */
  public ChannelAuthenticator(GrpcChannelKey channelKey, Subject subject,
      AlluxioConfiguration conf) {
    mChannelKey = channelKey;
    mUseSubject = true;
    mParentSubject = subject;
    mConfiguration = conf;
    mAuthType = conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    mGrpcAuthTimeoutMs = conf.getMs(PropertyKey.NETWORK_CONNECTION_AUTH_TIMEOUT);
  }

  /**
   * Creates {@link ChannelAuthenticator} instance.
   *
   * @param channelKey channel key
   * @param userName user name
   * @param password user password
   * @param impersonationUser impersonation user
   * @param authType authentication type
   * @param grpcAuthTimeoutMs authentication timeout in milliseconds
   */
  public ChannelAuthenticator(GrpcChannelKey channelKey, String userName, String password,
      String impersonationUser, AuthType authType, long grpcAuthTimeoutMs) {
    mChannelKey = channelKey;
    mUseSubject = false;
    mUserName = userName;
    mPassword = password;
    mImpersonationUser = impersonationUser;
    mAuthType = authType;
    mGrpcAuthTimeoutMs = grpcAuthTimeoutMs;
  }

  /**
   * Authenticates given {@link NettyChannelBuilder} instance. It attaches required interceptors to
   * the channel based on authentication type.
   *
   * @param managedChannel the managed channel for which authentication is taking place
   * @return channel that is augmented for authentication
   * @throws UnauthenticatedException
   */
  public AuthenticatedChannel authenticate(ManagedChannel managedChannel)
      throws AlluxioStatusException {
    LOG.debug("Channel authentication initiated. ChannelKey:{}, AuthType:{}, Target:{}",
        mChannelKey, mAuthType, managedChannel.authority());

    return new DefaultAuthenticatedChannel(managedChannel);
  }

  private class DefaultAuthenticatedChannel extends AuthenticatedChannel {
    /** Given managed channel reference. */
    private final ManagedChannel mManagedChannel;
    /** Augmented channel with authentication interceptors. */
    private Channel mChannel;
    /** Whether the channel is currently authenticated. */
    private AtomicBoolean mAuthenticated;
    /** Sasl traffic driver for the client. */
    private SaslStreamClientDriver mClientDriver;

    DefaultAuthenticatedChannel(ManagedChannel managedChannel)
        throws AlluxioStatusException {
      mManagedChannel = managedChannel;
      mAuthenticated = new AtomicBoolean(false);
      authenticate();
    }

    public void authenticate() throws AlluxioStatusException {
      // Determine channel authentication scheme to use.
      ChannelAuthenticationScheme authScheme =
          getChannelAuthScheme(mParentSubject, mChannelKey.getServerAddress().getSocketAddress());
      try (
          // Create SaslHandler for talking with target host's authentication service.
          SaslClientHandler saslClientHandler =
              createSaslClientHandler(mChannelKey.getServerAddress(), authScheme, mParentSubject)) {
        // Create authentication scheme specific handshake handler.
        SaslHandshakeClientHandler handshakeClient =
            new DefaultSaslHandshakeClientHandler(saslClientHandler);
        // Create driver for driving sasl traffic from client side.
        mClientDriver = new SaslStreamClientDriver(handshakeClient, mAuthenticated, mChannelKey,
            mGrpcAuthTimeoutMs);
        // Start authentication call with the service and update the client driver.
        StreamObserver<SaslMessage> requestObserver =
            SaslAuthenticationServiceGrpc.newStub(mManagedChannel).authenticate(mClientDriver);
        mClientDriver.setServerObserver(requestObserver);
        // Start authentication traffic with the target.
        // Successful return from this method means success.
        mClientDriver.start();
        // Intercept authenticated channel with channel-Id injector.
        mChannel = ClientInterceptors.intercept(mManagedChannel,
            new ChannelIdInjector(mChannelKey.getChannelId()));
      } catch (IOException e) {
        Status.Code code = Status.Code.UNKNOWN;
        if (e instanceof AlluxioStatusException) {
          code = ((AlluxioStatusException) e).getStatusCode();
        }
        String message = String.format(
            "Channel authentication failed with code:%s. ChannelKey: %s, AuthType: %s, Error: %s",
            code.name(), mChannelKey.toStringShort(), mAuthType, e.toString());
        throw AlluxioStatusException
            .from(Status.fromCode(code).withDescription(message).withCause(e));
      }
    }

    /**
     * Determines transport level authentication scheme for given subject.
     *
     * @param subject the subject
     * @param serverAddress the target server address
     * @return the channel authentication scheme to use
     * @throws UnauthenticatedException if configured authentication type is not supported
     */
    private ChannelAuthenticationScheme getChannelAuthScheme(Subject subject,
        SocketAddress serverAddress) throws UnauthenticatedException {
      switch (mAuthType) {
        case NOSASL:
          return ChannelAuthenticationScheme.NOSASL;
        case SIMPLE:
          return ChannelAuthenticationScheme.SIMPLE;
        case CUSTOM:
          return ChannelAuthenticationScheme.CUSTOM;
        default:
          throw new UnauthenticatedException(String.format(
                  "Configured authentication type is not supported: %s", mAuthType.getAuthName()));
      }
    }

    /**
     * Create SaslClient handler for authentication.
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
          if (mUseSubject) {
            return new alluxio.security.authentication.plain.SaslClientHandlerPlain(mParentSubject,
                    mConfiguration);
          } else {
            return new alluxio.security.authentication.plain.SaslClientHandlerPlain(mUserName,
                    mPassword, mImpersonationUser);
          }
        default:
          throw new UnauthenticatedException(
              String.format("Channel authentication scheme not supported: %s", authScheme.name()));
      }
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
        MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
      return mChannel.newCall(methodDescriptor, callOptions);
    }

    @Override
    public String authority() {
      return mChannel.authority();
    }

    @Override
    public boolean isAuthenticated() {
      return mAuthenticated.get();
    }

    @Override
    public UUID getChannelId() {
      return mChannelKey.getChannelId();
    }

    @Override
    public void close() {
      // Stopping client driver will close authentication with the server.
      mClientDriver.stop();
    }
  }
}
