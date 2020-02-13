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

package alluxio.grpc;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedChannelClientDriver;
import alluxio.security.authentication.ChannelAuthenticator;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.netty.channel.EventLoopGroup;

import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

/**
 * A gRPC channel builder that authenticates with {@link GrpcServer} at the target during channel
 * building.
 */
public final class GrpcChannelBuilder {
  /** gRPC channel key. */
  private final GrpcChannelKey mChannelKey;

  /** Subject for authentication. */
  private Subject mParentSubject;

  /** Whether to authenticate the channel with the server. */
  private boolean mAuthenticateChannel;

  // Configuration constants.
  private final AuthType mAuthType;
  private final long mShutdownTimeoutMs;
  private final long mHealthCheckTimeoutMs;

  private AlluxioConfiguration mConfiguration;

  private GrpcChannelBuilder(GrpcServerAddress address, AlluxioConfiguration conf) {
    mConfiguration = conf;

    // Read constants.
    mAuthType =
        mConfiguration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    mShutdownTimeoutMs =
        mConfiguration.getMs(PropertyKey.NETWORK_CONNECTION_SHUTDOWN_TIMEOUT);
    mHealthCheckTimeoutMs =
        mConfiguration.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);

    // Set default overrides for the channel.
    mChannelKey = GrpcChannelKey.create(conf);
    mChannelKey.setServerAddress(address);
    mChannelKey.setMaxInboundMessageSize(
        (int) mConfiguration.getBytes(PropertyKey.USER_NETWORK_MAX_INBOUND_MESSAGE_SIZE));

    // Determine if authentication required.
    mAuthenticateChannel = mAuthType != AuthType.NOSASL;
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
   * Sets human readable name for the channel's client.
   *
   * @param clientType client type
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder setClientType(String clientType) {
    mChannelKey.setClientType(clientType);
    return this;
  }

  /**
   * Sets {@link Subject} for authentication.
   *
   * @param subject the subject
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder setSubject(Subject subject) {
    mParentSubject = subject;
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
  public GrpcChannelBuilder setPoolingStrategy(GrpcChannelKey.PoolingStrategy strategy) {
    mChannelKey.setPoolingStrategy(strategy);
    return this;
  }

  /**
   * Creates an authenticated channel of type {@link GrpcChannel}.
   *
   * @return the built {@link GrpcChannel}
   */
  public GrpcChannel build() throws AlluxioStatusException {
    // Acquire a managed channel from the pool.
    ManagedChannel managedChannel = GrpcManagedChannelPool.INSTANCE()
        .acquireManagedChannel(mChannelKey, mHealthCheckTimeoutMs, mShutdownTimeoutMs);
    try {
      Channel logicalChannel = managedChannel;
      AuthenticatedChannelClientDriver authDriver = null;
      if (mAuthenticateChannel) {
        // Create channel authenticator based on provided content.
        ChannelAuthenticator channelAuthenticator = new ChannelAuthenticator(mChannelKey,
            managedChannel, mParentSubject, mAuthType, mConfiguration);
        // Authenticate a new logical channel.
        channelAuthenticator.authenticate();
        // Acquire the authenticated logical channel.
        logicalChannel = channelAuthenticator.getAuthenticatedChannel();
        // Acquire authentication driver.
        authDriver = channelAuthenticator.getAuthenticationDriver();
      }
      // Return a wrapper over logical channel.
      return new GrpcChannel(mChannelKey, logicalChannel, mShutdownTimeoutMs, authDriver);
    } catch (Throwable t) {
      // Release the managed channel to the pool.
      GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(mChannelKey, mShutdownTimeoutMs);
      // Pretty print unavailable cases.
      if (t instanceof UnavailableException) {
        throw new UnavailableException(
            String.format("Target Unavailable. %s", mChannelKey.toStringShort()), t.getCause());
      }
      throw AlluxioStatusException.fromThrowable(t);
    }
  }
}
