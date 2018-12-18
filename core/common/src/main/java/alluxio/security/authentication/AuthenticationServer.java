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

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import javax.security.sasl.SaslServer;
import java.util.List;
import java.util.UUID;

/**
 * Interface for authentication server implementations.
 */
public interface AuthenticationServer extends BindableService {
  /**
   * Registers new user against given channel.
   *
   * @param channelId channel id
   * @param authorizedUser authorized user name
   * @param saslServer server that has been used for authentication
   */
  public void registerChannel(UUID channelId, String authorizedUser, SaslServer saslServer);

  /**
   * @param channelId channel id
   * @return user name associated with the given channel
   * @throws UnauthenticatedException if given channel is not registered
   */
  public String getUserNameForChannel(UUID channelId) throws UnauthenticatedException;

  /**
   * Unregisters given channel.
   *
   * @param channelId channel id
   */
  public void unregisterChannel(UUID channelId);

  /**
   * @return list of server-side interceptors that are required for configured authentication type
   */
  public List<ServerInterceptor> getInterceptors();
}
