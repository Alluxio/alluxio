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

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.model.Statement;

/**
 * Unit tests for {@link ConfigurationRule}.
 */
public final class LoginUserRuleTest {
  private static final String TESTCASE_USER = "testcase-user";
  private static final String RULE_USER = "rule-user";
  private static final String OUTSIDE_RULE_USER = "outside-rule-user";

  private final Statement mStatement = new Statement() {
    @Override
    public void evaluate() throws Throwable {
      Assert.assertEquals(RULE_USER, LoginUser.get().getName());
      LoginUserTestUtils.resetLoginUser(TESTCASE_USER);
      Assert.assertEquals(TESTCASE_USER, LoginUser.get().getName());
    }
  };

  @After
  public void after() throws Exception {
    LoginUserTestUtils.resetLoginUser();
  }

  @Test
  public void userSetBeforeRule() throws Throwable {
    LoginUserTestUtils.resetLoginUser(OUTSIDE_RULE_USER);
    new LoginUserRule(RULE_USER).apply(mStatement, null).evaluate();
    Assert.assertEquals(OUTSIDE_RULE_USER, LoginUser.get().getName());
  }

  @Test
  public void noUserBeforeRule() throws Throwable {
    LoginUserTestUtils.resetLoginUser();
    String user = LoginUser.get().getName();
    new LoginUserRule(RULE_USER).apply(mStatement, null).evaluate();
    Assert.assertEquals(user, LoginUser.get().getName());
  }
}
