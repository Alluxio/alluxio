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

import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;

import io.grpc.BindableService;

import javax.security.sasl.SaslException;
import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

/**
 * Interface for authentication server implementations.
 */
public interface AuthenticationServer extends BindableService, Closeable {
  /**
   * Registers new user against given channel.
   *
   * @param channelId channel id
   * @param userInfo authanticated user info
   * @param saslDriver sasl server driver
   */
  void registerChannel(UUID channelId, AuthenticatedUserInfo userInfo,
      AuthenticatedChannelServerDriver saslDriver);

  /**
   * @param channelId channel id
   * @return info of user that is authenticated with the given channel
   * @throws UnauthenticatedException if given channel is not registered
   */
  AuthenticatedUserInfo getUserInfoForChannel(UUID channelId) throws UnauthenticatedException;

  /**
   * Unregisters given channel.
   *
   * @param channelId channel id
   */
  void unregisterChannel(UUID channelId);

  /**
   * Creates server-side Sasl handler for given scheme.
   *
   * @param scheme the authentication scheme
   * @return the created {@link SaslServerHandler} instance
   * @throws SaslException
   */
  SaslServerHandler createSaslHandler(ChannelAuthenticationScheme scheme) throws SaslException;

  /**
   * Closes the server, releases all authentication sessions.
   */
  void close() throws IOException;
}
