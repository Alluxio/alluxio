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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * This class is the main entry for Tachyon authentication.
 * It switches different modes based on configuration, and provides corresponding Thrift class
 * for authenticated connection between Client and Server.
 */
public class AuthenticationFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Different authentication types for Tachyon.
   */
  public enum AuthType {
    /**
     * Authentication is disabled. No user info in Tachyon.
     */
    NOSASL("NOSASL"),

    /**
     * User is aware in Tachyon.
     * Login user is OS user. The verification of client user is disabled.
     */
    SIMPLE("SIMPLE"),

    /**
     * User is aware in Tachyon.
     * The user is verified by Kerberos authentication.
     */
    KERBEROS("KERBEROS");

    private final String mAuthType;

    AuthType(String authType) {
      mAuthType = authType;
    }

    public String getAuthName() {
      return mAuthType;
    }

    public static AuthType getValidAuthType(String authTypeStr) {
      for (AuthType authType : AuthType.values()) {
        if (authTypeStr.equalsIgnoreCase(authType.getAuthName())) {
          return authType;
        }
      }
      throw new IllegalArgumentException("Not a valid authentication type: " + authTypeStr);
    }
  }

  private final String mAuthTypeStr;
  private final TachyonConf mTachyonConf;

  public AuthenticationFactory(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    // TODO: change the default value from NOSASL to SIMPLE, after feature is stable.
    mAuthTypeStr = tachyonConf.get(Constants.TACHYON_SECURITY_AUTHENTICATION,
        AuthType.NOSASL.getAuthName());
  }

  String getAuthTypeStr() {
    return mAuthTypeStr;
  }

  public static AuthType getAuthTypeFromConf(TachyonConf conf) {
    // TODO: change the default value from NOSASL to SIMPLE, after feature is stable.
    return AuthType.getValidAuthType(conf.get(Constants.TACHYON_SECURITY_AUTHENTICATION,
        AuthType.NOSASL.getAuthName()));
  }

  // TODO: add methods of getting different Thrift class in follow-up PR.
}
