/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.security.authentication;

import javax.security.sasl.AuthenticationException;

import tachyon.conf.TachyonConf;

public interface AuthenticationProvider {
  /**
   * Factory method to create an instance of {@link AuthenticationProvider} based on
   * {@link AuthType} and {@link TachyonConf}.
   */
  class Factory {
    public static AuthenticationProvider getAuthenticationProvider(AuthType authType,
        TachyonConf conf) throws AuthenticationException {
      switch (authType) {
        case SIMPLE:
          return new SimpleAuthenticationProviderImpl();
        case CUSTOM:
          return new CustomAuthenticationProviderImpl(conf);
        default:
          throw new AuthenticationException("Unsupported AuthType: " + authType.getAuthName());
      }
    }
  }

  /**
   * The authenticate method is called by the PlainServerCallbackHandler in the PlainSaslServer
   * layer to authenticate users for their requests. If a user is to be granted, return
   * nothing/throw nothing. When a user is to be disallowed, throw an appropriate
   * {@link AuthenticationException}.
   *
   * @param user The username received over the connection request
   * @param password The password received over the connection request
   *
   * @throws AuthenticationException When a user is found to be invalid by the implementation
   */
  void authenticate(String user, String password) throws AuthenticationException;
}
