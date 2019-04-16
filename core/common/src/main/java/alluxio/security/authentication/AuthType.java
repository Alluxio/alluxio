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

import javax.annotation.concurrent.ThreadSafe;

/**
 * Different authentication types for Alluxio.
 */
@ThreadSafe
public enum AuthType {
  /**
   * Authentication is disabled. No user info in Alluxio.
   */
  NOSASL,

  /**
   * User is aware in Alluxio. On the client side, the login username is determined by the
   * "alluxio.security.login.username" property, or the OS user upon failure.
   * On the server side, the verification of client user is disabled.
   */
  SIMPLE,

  /**
   * User is aware in Alluxio. On the client side, the login username is determined by the
   * "alluxio.security.login.username" property, or the OS user upon failure.
   * On the server side, the user is verified by a Custom authentication provider
   * (Specified by property "alluxio.security.authentication.custom.provider.class").
   */
  CUSTOM,

  /**
   * User is aware in Alluxio. The user is verified by Kerberos authentication. NOTE: this
   * authentication is not supported.
   */
  KERBEROS,
  ;

  /**
   * @return the string value of AuthType
   */
  public String getAuthName() {
    return name();
  }
}
