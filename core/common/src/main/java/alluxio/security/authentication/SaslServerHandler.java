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

import java.util.UUID;

import javax.security.sasl.SaslServer;

/**
 * Interface for authentication scheme specific {@link SaslServer} management.
 */
public interface SaslServerHandler {
  /**
   * Callback that will be called when authentication complete.
   *
   * @param channelId channel that has been authenticated
   * @param authenticationServer authentication server instance
   */
  void authenticationCompleted(UUID channelId, AuthenticationServer authenticationServer);

  /**
   * @return the {@link SaslServer} instance
   */
  SaslServer getSaslServer();

  /**
   * To be called by callbacks to store authenticated user information.
   *
   * @param userinfo user info
   */
  void setAuthenticatedUserInfo(AuthenticatedUserInfo userinfo);
}
