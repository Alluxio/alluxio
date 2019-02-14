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
import alluxio.exception.status.UnknownException;
import alluxio.grpc.SaslAuthenticationServiceGrpc;
import alluxio.grpc.SaslMessage;
import alluxio.util.SecurityUtils;
import alluxio.grpc.GrpcChannelBuilder;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.sasl.SaslClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Used to authenticate with the target host. Used internally by {@link GrpcChannelBuilder}.
 */
public class ChannelAuthenticator {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelAuthenticator.class);
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

  /** Internal ID used to identify the channel that is being authenticated. */
  protected UUID mChannelId;

  private boolean mSecurityEnabled;

  /**
   * Creates {@link ChannelAuthenticator} instance.
   *
   * @param subject javax subject to use for authentication
   * @param conf Alluxio configuration
   */
  public ChannelAuthenticator(Subject subject, AlluxioConfiguration conf) {
    mUseSubject = true;
    mChannelId = UUID.randomUUID();
    mParentSubject = subject;
    mAuthType = conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    mSecurityEnabled = SecurityUtils.isSecurityEnabled(conf);
    mGrpcAuthTimeoutMs = conf.getMs(PropertyKey.MASTER_GRPC_CHANNEL_AUTH_TIMEOUT);
  }

  /**
   * Creates {@link ChannelAuthenticator} instance.
   *
   * @param userName user name
   * @param password user password
   * @param impersonationUser impersonation user
   * @param authType authentication type
   * @param grpcAuthTimeoutMs authentication timeout in milliseconds
   */
  public ChannelAuthenticator(String userName, String password, String impersonationUser,
      AuthType authType, long grpcAuthTimeoutMs) {
    mUseSubject = false;
    mChannelId = UUID.randomUUID();
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
   * @param managedChannel the managed channel for whch authentication is taking place
   * @param conf Alluxio configuration
   * @return channel that is augmented for authentication
   * @throws UnauthenticatedException
   */
  public Channel authenticate(ManagedChannel managedChannel, AlluxioConfiguration conf)
      throws AlluxioStatusException {
    LOG.debug("Channel authentication initiated. ChannelId:{}, AuthType:{}, Target:{}", mChannelId,
        mAuthType, managedChannel.authority());

    if (mAuthType == AuthType.NOSASL) {
      return managedChannel;
    }

    AuthenticatedManagedChannel authenticatedChannel =
        new AuthenticatedManagedChannel(managedChannel, conf);
    return authenticatedChannel;
  }

  /**
   * @param saslClient the Sasl client object that have been used for authentication
   * @return the list of interceptors that are required for configured authentication
   */
  private List<ClientInterceptor> getInterceptors(SaslClient saslClient) {
    if (!mSecurityEnabled) {
      return Collections.emptyList();
    }
    List<ClientInterceptor> interceptorsList = new ArrayList<>();
    switch (mAuthType) {
      case NOSASL:
        break;
      case SIMPLE:
      case CUSTOM:
        // Plug channel id augmenting for SIMPLE/CUSTOM auth schemes.
        interceptorsList.add(new ChannelIdInjector(mChannelId));
        break;
      default:
        throw new RuntimeException(
            String.format("Authentication type:%s not supported", mAuthType.name()));
    }
    return interceptorsList;
  }

  private class AuthenticatedManagedChannel extends Channel implements AuthenticatedChannel {
    private final ManagedChannel mManagedChannel;
    private final AlluxioConfiguration mConf;
    private Channel mChannel;
    private boolean mAuthenticated;

    AuthenticatedManagedChannel(ManagedChannel managedChannel, AlluxioConfiguration conf)
        throws AlluxioStatusException {
      mManagedChannel = managedChannel;
      mConf = conf;
      authenticate();
    }

    public void authenticate() throws AlluxioStatusException {
      try {
        // Create a channel for talking with target host's authentication service.
        // Create SaslClient for authentication based on provided credentials.
        SaslClient saslClient;
        if (mUseSubject) {
          saslClient = SaslParticipantProvider.Factory.create(mAuthType)
              .createSaslClient(mParentSubject, mConf);
        } else {
          saslClient = SaslParticipantProvider.Factory.create(mAuthType).createSaslClient(mUserName,
              mPassword, mImpersonationUser);
        }

        // Create authentication scheme specific handshake handler.
        SaslHandshakeClientHandler handshakeClient =
            SaslHandshakeClientHandler.Factory.create(mAuthType, saslClient);
        // Create driver for driving sasl traffic from client side.
        SaslStreamClientDriver clientDriver =
            new SaslStreamClientDriver(handshakeClient, mGrpcAuthTimeoutMs);
        // Start authentication call with the service and update the client driver.
        StreamObserver<SaslMessage> requestObserver =
            SaslAuthenticationServiceGrpc.newStub(mManagedChannel).authenticate(clientDriver);
        clientDriver.setServerObserver(requestObserver);
        // Start authentication traffic with the target.
        clientDriver.start(mChannelId.toString());
        // Authentication succeeded!
        mAuthenticated = true;
        mManagedChannel.notifyWhenStateChanged(ConnectivityState.READY, () -> {
          mAuthenticated = false;
        });
        // Attach scheme specific interceptors to the channel.
        mChannel = ClientInterceptors.intercept(mManagedChannel, getInterceptors(saslClient));
      } catch (Exception exc) {
        String message = String.format(
            "Channel authentication failed. ChannelId: %s, AuthType: %s, Target: %s, Error: %s",
            mChannelId, mAuthType, mManagedChannel.authority(), exc.toString());
        if (exc instanceof AlluxioStatusException) {
          throw AlluxioStatusException.from(((AlluxioStatusException) exc).getStatus(), message,
              exc);
        } else {
          throw new UnknownException(message, exc);
        }
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
      return mAuthenticated;
    }
  }
}
