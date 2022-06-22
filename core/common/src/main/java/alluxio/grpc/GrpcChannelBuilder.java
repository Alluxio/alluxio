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
import alluxio.security.authentication.AuthenticationUtils;

import javax.security.auth.Subject;

/**
 * A gRPC channel builder that authenticates with {@link GrpcServer} at the target during channel
 * building.
 */
public final class GrpcChannelBuilder {
  private final GrpcServerAddress mAddress;
  private final AlluxioConfiguration mConfiguration;

  private Subject mParentSubject;
  private AuthType mAuthType;
  private GrpcNetworkGroup mNetworkGroup = GrpcNetworkGroup.RPC;

  private GrpcChannelBuilder(GrpcServerAddress address, AlluxioConfiguration conf) {
    mAddress = address;
    mConfiguration = conf;
    mAuthType =
        conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
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
    return this;
  }

  /**
   * Disables authentication with the server.
   *
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder disableAuthentication() {
    mAuthType = AuthType.NOSASL;
    return this;
  }

  /**
   * Sets the pooling strategy.
   *
   * @param group the networking group
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public GrpcChannelBuilder setNetworkGroup(GrpcNetworkGroup group) {
    mNetworkGroup = group;
    return this;
  }

  /**
   * Creates an authenticated channel of type {@link GrpcChannel}.
   *
   * @return the built {@link GrpcChannel}
   */
  public GrpcChannel build() throws AlluxioStatusException {
    GrpcChannelKey channelKey = new GrpcChannelKey(mNetworkGroup, mAddress);
    // Acquire a connection from the pool.
    GrpcConnection connection =
        GrpcConnectionPool.INSTANCE.acquireConnection(channelKey, mConfiguration);
    try {
      if (mAuthType != AuthType.NOSASL) {
        return new GrpcChannel(connection,
            AuthenticationUtils.authenticate(
                connection, mParentSubject, mAuthType, mConfiguration));
      }
      // Return a wrapper over logical channel.
      return new GrpcChannel(connection, null);
    } catch (Throwable t) {
      try {
        connection.close();
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to release the connection. " + channelKey, e);
      }
      // Pretty print unavailable cases.
      if (t instanceof UnavailableException) {
        throw new UnavailableException(String.format("Failed to connect to remote server %s. %s",
            channelKey.getServerAddress(), channelKey),
            t.getCause());
      }
      throw AlluxioStatusException.fromThrowable(t);
    }
  }
}
