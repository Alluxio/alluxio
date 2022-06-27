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
import alluxio.security.authentication.AuthType;

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
    // Acquire a connection from the pool.
    GrpcChannel channel =
        GrpcChannelPool.INSTANCE.acquireChannel(mNetworkGroup, mAddress, mConfiguration);
    try {
      channel.authenticate(mAuthType, mParentSubject, mConfiguration);
    } catch (Throwable t) {
      try {
        channel.close();
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to release the connection: %s", channel.getChannelKey()), e);
      }
      if (t instanceof AlluxioStatusException) {
        throw t;
      }
      throw AlluxioStatusException.fromThrowable(t);
    }
    return channel;
  }
}
