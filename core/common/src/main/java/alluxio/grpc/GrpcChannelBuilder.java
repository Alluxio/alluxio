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
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.ChannelBuilderAuthenticator;
import io.grpc.ClientInterceptor;
import io.grpc.netty.NettyChannelBuilder;

import javax.security.auth.Subject;
import javax.security.sasl.AuthenticationException;
import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * A gRPC channel builder that authenticates with {@link GrpcServer} at the target
 * during channel building.
 */
public final class GrpcChannelBuilder {

  NettyChannelBuilder mChannelBuilder;
  InetSocketAddress mAddress;
  Subject mParentSubject;
  AuthType mAuthType;

  private GrpcChannelBuilder(InetSocketAddress address) {
    mAddress = address;
    mChannelBuilder = NettyChannelBuilder.forAddress(mAddress);
    mParentSubject = null;
    mAuthType = Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
  }

  /**
   * Create a channel builder for given address.
   *
   * @param address the host address
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public static GrpcChannelBuilder forAddress(InetSocketAddress address) {
    return new GrpcChannelBuilder(address).usePlaintext(true);
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
   * Whether to use plain text.
   *
   * @param skipNegotiation whether to skip negotiation
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder usePlaintext(boolean skipNegotiation) {
    mChannelBuilder = mChannelBuilder.usePlaintext(skipNegotiation);
    return this;
  }

  /**
   * Registers given client interceptor.
   * 
   * @param interceptor client interceptor
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder intercept(ClientInterceptor interceptor) {
    mChannelBuilder = mChannelBuilder.intercept(interceptor);
    return this;
  }

  /**
   * Creates an authenticated channel of type {@link GrpcChannel}.
   * 
   * @return the built {@link GrpcChannel}
   */
  public GrpcChannel build() throws AuthenticationException {
    ChannelBuilderAuthenticator channelAuthenticator =
        new ChannelBuilderAuthenticator(UUID.randomUUID(), mParentSubject, mAddress, mAuthType);
    return new GrpcChannel(channelAuthenticator.authenticate(mChannelBuilder).build());
  }
}
