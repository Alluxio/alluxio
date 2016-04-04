/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.Constants;

import javax.security.sasl.AuthenticationException;

/**
 * Abstraction for an authentication provider for different authentication types.
 */
public interface AuthenticationProvider {

  /**
   * Factory for {@link AuthenticationProvider}.
   */
  class Factory {
    /**
     * @param authType authentication type to use
     * @param conf Alluxio configuration
     * @return the generated {@link AuthenticationProvider}
     * @throws AuthenticationException when unsupported authentication type is used
     */
    public static AuthenticationProvider create(AuthType authType, Configuration conf)
        throws AuthenticationException {
      switch (authType) {
        case SIMPLE:
          return new SimpleAuthenticationProvider();
        case CUSTOM:
          String customProviderName = conf.get(Constants.SECURITY_AUTHENTICATION_CUSTOM_PROVIDER);
          return new CustomAuthenticationProvider(customProviderName);
        default:
          throw new AuthenticationException("Unsupported AuthType: " + authType.getAuthName());
      }
    }
  }

  /**
   * The authenticate method is called by the {@link PlainSaslServer.PlainServerCallbackHandler} in
   * the {@link PlainSaslServer} layer to authenticate users for their requests. If a user is to be
   * granted, return nothing/throw nothing. When a user is to be disallowed, throw an appropriate
   * {@link AuthenticationException}.
   *
   * @param user The username received over the connection request
   * @param password The password received over the connection request
   *
   * @throws AuthenticationException when a user is found to be invalid by the implementation
   */
  void authenticate(String user, String password) throws AuthenticationException;
}
