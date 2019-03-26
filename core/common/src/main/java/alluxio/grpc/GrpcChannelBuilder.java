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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.ChannelAuthenticator;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.netty.channel.EventLoopGroup;

import javax.security.auth.Subject;
import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * A gRPC channel builder that authenticates with {@link GrpcServer} at the target during channel
 * building.
 */
public final class GrpcChannelBuilder {
  /** Key for acquiring the underlying managed channel. */
  protected GrpcManagedChannelPool.ChannelKey mChannelKey;

  /** gRPC Server address. */
  protected GrpcServerAddress mServerAddress;

  /** Whether to use mParentSubject as authentication user. */
  protected boolean mUseSubject;
  /** Subject for authentication. */
  protected Subject mParentSubject;

  /* Used in place of a subject. */
  protected String mUserName;
  protected String mPassword;
  protected String mImpersonationUser;

  /** Whether to authenticate the channel with the server. */
  protected boolean mAuthenticateChannel;

  protected AlluxioConfiguration mConfiguration;

  private GrpcChannelBuilder(GrpcServerAddress address, AlluxioConfiguration conf) {
    mServerAddress = address;
    mConfiguration = conf;
    mChannelKey = GrpcManagedChannelPool.ChannelKey.create(conf);
    // Set default overrides for the channel.
    mChannelKey.setAddress(address.getSocketAddress());
    mChannelKey.setMaxInboundMessageSize(
        (int) mConfiguration.getBytes(PropertyKey.USER_NETWORK_MAX_INBOUND_MESSAGE_SIZE));
    mUseSubject = true;
    mAuthenticateChannel = true;
  }

  /**
   * Create a channel builder for given address using the given configuration.
   *
   * @param address the host address
   * @param conf Alluxio configuration
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public static GrpcChannelBuilder newBuilder(GrpcServerAddress address,
      AlluxioConfiguration conf) {
    return new GrpcChannelBuilder(address, conf);
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
   * Sets the time to wait after receiving last message before pinging the server.
   *
   * @param keepAliveTime the time to wait after receiving last message before pinging server
   * @param timeUnit unit of the time
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder setKeepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
    mChannelKey.setKeepAliveTime(keepAliveTime, timeUnit);
    return this;
  }

  /**
   * Sets the maximum time waiting for response after pinging server before closing connection.
   *
   * @param keepAliveTimeout the time to wait after pinging server before closing the connection
   * @param timeUnit unit of the timeout
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
   * Sets the pooling strategy.
   *
   * @param strategy the pooling strategy
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public GrpcChannelBuilder setPoolingStrategy(GrpcManagedChannelPool.PoolingStrategy strategy) {
    mChannelKey.setPoolingStrategy(strategy);
    return this;
  }

  /**
   * Creates an authenticated channel of type {@link GrpcChannel}.
   *
   * @return the built {@link GrpcChannel}
   */
  public GrpcChannel build() throws AlluxioStatusException {
    ManagedChannel underlyingChannel =
        GrpcManagedChannelPool.INSTANCE().acquireManagedChannel(mChannelKey,
            mConfiguration.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT_MS),
            mConfiguration.getMs(PropertyKey.MASTER_GRPC_CHANNEL_SHUTDOWN_TIMEOUT));
    try {
      Channel clientChannel = underlyingChannel;

      if (mAuthenticateChannel) {
        // Create channel authenticator based on provided content.
        ChannelAuthenticator channelAuthenticator;
        if (mUseSubject) {
          channelAuthenticator = new ChannelAuthenticator(mParentSubject, mConfiguration);
        } else {
          channelAuthenticator =
              new ChannelAuthenticator(mUserName, mPassword, mImpersonationUser,
                  mConfiguration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class),
                  mConfiguration.getMs(PropertyKey.MASTER_GRPC_CHANNEL_AUTH_TIMEOUT));
        }
        // Get an authenticated wrapper channel over given managed channel.
        clientChannel = channelAuthenticator.authenticate(mServerAddress, underlyingChannel);
      }
      // Create the channel after authentication with the target.
      return new GrpcChannel(mChannelKey, clientChannel,
          mConfiguration.getMs(PropertyKey.MASTER_GRPC_CHANNEL_SHUTDOWN_TIMEOUT));
    } catch (Exception exc) {
      // Release the managed channel to the pool before throwing.
      GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(mChannelKey,
          mConfiguration.getMs(PropertyKey.MASTER_GRPC_CHANNEL_SHUTDOWN_TIMEOUT));
      throw exc;
    }
  }
}
