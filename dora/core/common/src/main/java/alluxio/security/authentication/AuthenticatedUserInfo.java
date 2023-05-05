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

import com.google.common.base.MoreObjects;

/**
 * Used to define an authenticated user.
 */
public class AuthenticatedUserInfo {
  private String mAuthorizedUserName;
  private String mConnectionUserName;
  private String mAuthMethod;

  /**
   * Creates an {@link AuthenticatedUserInfo} instance.
   */
  public AuthenticatedUserInfo() {
    mAuthorizedUserName = null;
    mConnectionUserName = null;
    mAuthMethod = null;
  }

  /**
   * Creates an {@link AuthenticatedUserInfo} instance.
   * @param authorizedUserName authorized user name
   */
  public AuthenticatedUserInfo(String authorizedUserName) {
    this();
    mAuthorizedUserName = authorizedUserName;
  }

  /**
   * Creates an {@link AuthenticatedUserInfo} instance.
   * @param authorizedUserName authorized user name
   * @param connectionUserName connection user name
   */
  public AuthenticatedUserInfo(String authorizedUserName, String connectionUserName) {
    this(authorizedUserName);
    mConnectionUserName = connectionUserName;
  }

  /**
   * Creates an {@link AuthenticatedUserInfo} instance.
   * @param authorizedUserName authorized user name
   * @param connectionUserName connection user name
   * @param authMethod authentication method for the user
   */
  public AuthenticatedUserInfo(String authorizedUserName, String connectionUserName,
      String authMethod) {
    this(authorizedUserName, connectionUserName);
    mAuthMethod = authMethod;
  }

  /**
   * @return the authorized user
   */
  public String getAuthorizedUserName() {
    return mAuthorizedUserName;
  }

  /**
   * @return the connection user
   */
  public String getConnectionUserName() {
    return mConnectionUserName;
  }

  /**
   * @return authentication method
   */
  public String getAuthMethod() {
    return mAuthMethod;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("AuthorizedUser", mAuthorizedUserName)
        .add("ConnectionUser", mConnectionUserName)
        .add("AuthenticationMethod", mAuthMethod)
        .toString();
  }
}
