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
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslServer;

public class AuthenticationProviderFactory {
  public enum AuthenticationMethod {
    /**
     * SIMPLE AuthenticationMethod stands for PLAIN mechanism SASL negotiation
     * CUSTOM AuthenticationMethod stands for delegating the implementation of
     * {@link SaslServer} and {@link SaslClient} to user to customize the SASL
     * negotiation
     */
    SIMPLE("SIMPLE"),
    CUSTOM("CUSTOM");

    private final String mAuthMethod;

    AuthenticationMethod(String authMethod) {
      mAuthMethod = authMethod;
    }

    public String getmAuthMethod() {
      return mAuthMethod;
    }

    public static AuthenticationMethod getValidAuthenticationMethod(String authMethodStr) {
      for (AuthenticationMethod auth : AuthenticationMethod.values()) {
        if (authMethodStr.equalsIgnoreCase(auth.getmAuthMethod())) {
          return auth;
        }
      }
      throw new UnsupportedOperationException("Not a valid authentication method: "
          + authMethodStr);
    }
  }

  public static AuthenticationProvider getAuthenticationProvider(AuthenticationMethod method)
      throws AuthenticationException {
    if (method == AuthenticationMethod.SIMPLE) {
      return new AnonymousAuthenticationProviderImpl();
    } else if (method == AuthenticationMethod.CUSTOM) {
      return new CustomAuthenticationProviderImpl();
    } else {
      throw new AuthenticationException("Unsupported authentication method: " + method
          .getmAuthMethod());
    }
  }
}
