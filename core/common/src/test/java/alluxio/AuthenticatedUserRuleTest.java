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

import alluxio.security.authentication.AuthenticatedClientUser;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.model.Statement;

/**
 * Unit tests for {@link ConfigurationRule}.
 */
public final class AuthenticatedUserRuleTest {

  @Test
  public void changeConfiguration() throws Throwable {
    Statement statement = new Statement() {
      @Override
      public void evaluate() throws Throwable {
        Assert.assertEquals("testuser", AuthenticatedClientUser.get().getName());
        Assert.assertTrue(Configuration.containsKey(PropertyKey.SECURITY_LOGIN_USERNAME));
      }
    };
    Assert.assertNull(AuthenticatedClientUser.get());
    Assert.assertFalse(Configuration.containsKey(PropertyKey.SECURITY_LOGIN_USERNAME));
    new AuthenticatedUserRule("testuser").apply(statement, null).evaluate();
    Assert.assertNull(AuthenticatedClientUser.get());
    Assert.assertFalse(Configuration.containsKey(PropertyKey.SECURITY_LOGIN_USERNAME));
  }
}
