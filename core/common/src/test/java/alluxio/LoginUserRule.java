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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A rule for login an Alluxio user during a test suite. It sets
 * {@link PropertyKey#SECURITY_LOGIN_USERNAME} to the specified user name during the lifetime
 * of this rule.
 */
public final class LoginUserRule implements TestRule {
  private final String mUser;

  /**
   * @param user the user name to set as authenticated user
   */
  public LoginUserRule(String user) {
    mUser = user;
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try (SetAndRestoreLoginUser user = new SetAndRestoreLoginUser(mUser)) {
          statement.evaluate();
        }
      }
    };
  }
}
