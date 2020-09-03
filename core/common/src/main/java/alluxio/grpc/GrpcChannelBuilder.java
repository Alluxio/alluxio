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

  private AlluxioConfiguration mConfiguration;

  private GrpcChannelBuilder(GrpcServerAddress address, AlluxioConfiguration conf) {
    mConfiguration = conf;

    // Read constants.
    mAuthType =
        mConfiguration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);

    // Set default overrides for the channel.
    mChannelKey = GrpcChannelKey.create(conf);
    mChannelKey.setServerAddress(address);

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
   * Sets the pooling strategy.
   *
   * @param group the networking group
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public GrpcChannelBuilder setNetworkGroup(GrpcNetworkGroup group) {
    mChannelKey.setNetworkGroup(group);
    return this;
  }

  /**
   * Creates an authenticated channel of type {@link GrpcChannel}.
   *
   * @return the built {@link GrpcChannel}
   */
  public GrpcChannel build() throws AlluxioStatusException {
    // Acquire a connection from the pool.
    GrpcConnection connection =
        GrpcConnectionPool.INSTANCE.acquireConnection(mChannelKey, mConfiguration);
    try {
      AuthenticatedChannelClientDriver authDriver = null;
      if (mAuthenticateChannel) {
        // Create channel authenticator based on provided content.
        ChannelAuthenticator channelAuthenticator = new ChannelAuthenticator(
            connection, mParentSubject, mAuthType, mConfiguration);
        // Authenticate a new logical channel.
        channelAuthenticator.authenticate();
        // Acquire authentication driver.
        authDriver = channelAuthenticator.getAuthenticationDriver();
      }
      // Return a wrapper over logical channel.
      return new GrpcChannel(connection, authDriver);
    } catch (Throwable t) {
      try {
        connection.close();
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to release the connection. " + mChannelKey.toStringShort(), e);
      }
      // Pretty print unavailable cases.
      if (t instanceof UnavailableException) {
        throw new UnavailableException(String.format("Failed to connect to remote server %s. %s",
            mChannelKey.getServerAddress(), mChannelKey.toStringShort()),
            t.getCause());
      }
      throw AlluxioStatusException.fromThrowable(t);
    }
  }
}
