/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.grpc;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.ChannelAuthenticator;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.netty.channel.EventLoopGroup;

import javax.security.auth.Subject;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * A gRPC channel builder that authenticates with {@link GrpcServer} at the target during channel
 * building.
 */
public final class GrpcChannelBuilder {
  /** Key for acquiring the underlying managed channel. */
  protected GrpcManagedChannelPool.ChannelKey mChannelKey;

  /** Whether to use mnarentSubject as authentication user. */
  protected boolean mUseSubject;
  /** Subject for authentication. */
  protected Subject mParentSubject;

  /* Used in place of a subject. */
  protected String mUserName;
  protected String mPassword;
  protected String mImpersonationUser;

  /** Whether to authenticate the channel with the server. */
  protected boolean mAuthenticateChannel;
  /** Authentication type to use. */
  protected AuthType mAuthType;

  private GrpcChannelBuilder(SocketAddress address) {
    mChannelKey = GrpcManagedChannelPool.ChannelKey.create();
    mChannelKey.setAddress(address).usePlaintext();
    mUseSubject = true;
    mAuthenticateChannel = true;
    mAuthType = Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
  }

  /**
   * Create a channel builder for given address.
   *
   * @param address the host address
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public static GrpcChannelBuilder forAddress(SocketAddress address) {
    return new GrpcChannelBuilder(address);
  }

  /**
   * Sets {@link Subject} for authentication.
   * 
   * @param subject the subject
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder setSubject(Subject subject) {
    mParentSubject = subject;
    mUseSubject = true;
    return this;
  }

  /**
   * Sets authentication content. Calling this will reset the subject set by {@link #setSubject}.
   * 
   * @param userName the username
   * @param password the password
   * @param impersonationUser the impersonation user
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder setCredentials(String userName, String password,
      String impersonationUser) {
    mUserName = userName;
    mPassword = password;
    mImpersonationUser = impersonationUser;
    mUseSubject = false;
    return this;
  }

  /**
   * Disables authentication with the server.
   * 
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder disableAuthentication() {
    mAuthenticateChannel = false;
    return this;
  }

  /**
   * Sets the time waiting for read activity after sending a keepalive ping.
   *
   * @param keepAliveTimeout the timeout for waiting after keepalive ping
   * @param timeUnit the time unit for the keepalive timeout
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder setKeepAliveTimeout(long keepAliveTimeout, TimeUnit timeUnit) {
    mChannelKey.setKeepAliveTimeout(keepAliveTimeout, timeUnit);
    return this;
  }

  /**
   * Sets the maximum message size allowed for a single gRPC frame.
   *
   * @param maxInboundMessaageSize the maximum inbound message size in bytes
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public GrpcChannelBuilder setMaxInboundMessageSize(int maxInboundMessaageSize) {
    mChannelKey.setMaxInboundMessageSize(maxInboundMessaageSize);
    return this;
  }

  /**
   * Sets the flow control window.
   *
   * @param flowControlWindow the flow control window in bytes
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public GrpcChannelBuilder setFlowControlWindow(int flowControlWindow) {
    mChannelKey.setFlowControlWindow(flowControlWindow);
    return this;
  }

  /**
   * Sets the channel type.
   *
   * @param channelType the channel type
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public GrpcChannelBuilder setChannelType(Class<? extends io.netty.channel.Channel> channelType) {
    mChannelKey.setChannelType(channelType);
    return this;
  }

  /**
   * Sets the event loop group.
   *
   * @param group the event loop group
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public GrpcChannelBuilder setEventLoopGroup(EventLoopGroup group) {
    mChannelKey.setEventLoopGroup(group);
    return this;
  }

  /**
   * Creates an authenticated channel of type {@link GrpcChannel}.
   * 
   * @return the built {@link GrpcChannel}
   */
  public GrpcChannel build() throws UnauthenticatedException {
    ManagedChannel underlyingChannel = GrpcManagedChannelPool.acquireManagedChannel(mChannelKey);
    Channel clientChannel = underlyingChannel;

    if (mAuthenticateChannel) {
      // Create channel authenticator based on provided content.
      ChannelAuthenticator channelAuthenticator;
      if (mUseSubject) {
        channelAuthenticator = new ChannelAuthenticator(mParentSubject, mAuthType);
      } else {
        channelAuthenticator =
            new ChannelAuthenticator(mUserName, mPassword, mImpersonationUser, mAuthType);
      }
      // Get an authenticated wrapper channel over given managed channel.
      clientChannel = channelAuthenticator.authenticate(underlyingChannel);
    }
    // Create the channel after authentication with the target.
    return new GrpcChannel(mChannelKey, clientChannel);
  }
}
