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

import static org.junit.Assert.assertEquals;

import alluxio.security.authentication.AuthenticatedClientUser;

import org.junit.After;
import org.junit.Test;
import org.junit.runners.model.Statement;

/**
 * Unit tests for {@link AuthenticatedUserRule}.
 */
public final class AuthenticatedUserRuleTest {
  private static final String TESTCASE_USER = "testcase-user";
  private static final String RULE_USER = "rule-user";
  private static final String OUTSIDE_RULE_USER = "outside-rule-user";

  private final Statement mStatement = new Statement() {
    @Override
    public void evaluate() throws Throwable {
      assertEquals(RULE_USER, AuthenticatedClientUser.get().getName());
      AuthenticatedClientUser.set(TESTCASE_USER);
      assertEquals(TESTCASE_USER, AuthenticatedClientUser.get().getName());
    }
  };

  @After
  public void after() throws Exception {
    AuthenticatedClientUser.remove();
  }

  @Test
  public void userSetBeforeRule() throws Throwable {
    AuthenticatedClientUser.set(OUTSIDE_RULE_USER);
    new AuthenticatedUserRule(RULE_USER).apply(mStatement, null).evaluate();
    assertEquals(OUTSIDE_RULE_USER, AuthenticatedClientUser.get().getName());
  }

  @Test
  public void noUserBeforeRule() throws Throwable {
    AuthenticatedClientUser.remove();
    new AuthenticatedUserRule(RULE_USER).apply(mStatement, null).evaluate();
    assertEquals(null, AuthenticatedClientUser.get());
  }
}
