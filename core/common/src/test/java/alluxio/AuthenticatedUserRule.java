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

import alluxio.conf.InstancedConfiguration;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A rule for login an Alluxio user during a test suite.
 * It sets {@link alluxio.security.authentication.AuthenticatedClientUser}
 * to the specified user name during the lifetime
 * of this rule. Note: setting the user only takes effect within the caller thread.
 *
 * NOTE: If this rule is used in conjunction with LocalAlluxioClusterResource, it must be ordered to
 * be second within a RuleChain.
 */
@NotThreadSafe
public final class AuthenticatedUserRule extends AbstractResourceRule {
  private final String mUser;
  private User mPreviousUser;
  private final InstancedConfiguration mConfiguration;

  /**
   * @param user the user name to set as authenticated user
   */
  public AuthenticatedUserRule(String user, InstancedConfiguration conf) {
    mUser = user;
    mConfiguration = conf;
  }

  @Override
  protected void before() throws Exception {
    mPreviousUser = AuthenticatedClientUser.get(mConfiguration);
    AuthenticatedClientUser.set(mUser);
  }

  @Override
  protected void after() {
    if (mPreviousUser == null) {
      AuthenticatedClientUser.remove();
    } else {
      AuthenticatedClientUser.set(mPreviousUser.getName());
    }
  }
}
