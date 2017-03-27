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

package alluxio;

import alluxio.security.LoginUser;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.User;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A rule for login an Alluxio user during a test suite. It sets
 * {@link PropertyKey#SECURITY_LOGIN_USERNAME} to the specified user name during the lifetime
 * of this rule.
 */
@NotThreadSafe
public final class LoginUserRule extends AbstractResourceRule {
  private final String mUser;
  private User mPreviousLoginUser = null;

  /**
   * @param user the user name to set as authenticated user
   */
  public LoginUserRule(String user) {
    mUser = user;
  }

  @Override
  public void before() throws Exception {
    mPreviousLoginUser = LoginUser.get();
    LoginUserTestUtils.resetLoginUser(mUser);
  }

  @Override
  public void after() {
    try {
      if (mPreviousLoginUser != null) {
        LoginUserTestUtils.resetLoginUser(mPreviousLoginUser.getName());
      } else {
        LoginUserTestUtils.resetLoginUser();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
