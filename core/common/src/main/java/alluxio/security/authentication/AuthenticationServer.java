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
import javax.security.sasl.SaslServer;
import java.util.UUID;

/**
 * Interface for authentication server implementations.
 */
public interface AuthenticationServer extends BindableService {
  /**
   * Registers new user against given channel.
   *
   * @param channelId channel id
   * @param userInfo authanticated user info
   * @param saslServer server that has been used for authentication
   */
  void registerChannel(UUID channelId, AuthenticatedUserInfo userInfo, SaslServer saslServer);

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
   * @throws UnauthenticatedException
   */
  SaslServerHandler createSaslHandler(ChannelAuthenticationScheme scheme)
      throws SaslException, UnauthenticatedException;
}
