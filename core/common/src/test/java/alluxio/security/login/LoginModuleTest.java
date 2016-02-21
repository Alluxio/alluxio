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

package alluxio.security.login;

import alluxio.security.User;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.security.Principal;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Unit test for the login modules defined in {@link AlluxioLoginModule} and
 * used in {@link LoginModuleConfiguration}.
 */
public class LoginModuleTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * This test verifies whether the simple login works in JAAS framework.
   * Simple mode login get the OS user and convert to Alluxio user.
   */
  @Test
  public void simpleLoginTest() throws Exception {
    String clazzName = LoginModuleConfigurationUtils.OS_PRINCIPAL_CLASS_NAME;
    @SuppressWarnings("unchecked")
    Class<? extends Principal> clazz = (Class<? extends Principal>) ClassLoader
        .getSystemClassLoader().loadClass(clazzName);
    Subject subject = new Subject();

    // login, add OS user into subject, and add corresponding Alluxio user into subject
    LoginContext loginContext = new LoginContext("simple", subject, null,
        new LoginModuleConfiguration());
    loginContext.login();

    // verify whether OS user and Alluxio user is added.
    Assert.assertFalse(subject.getPrincipals(clazz).isEmpty());
    Assert.assertFalse(subject.getPrincipals(User.class).isEmpty());

    // logout and verify the user is removed
    loginContext.logout();
    Assert.assertTrue(subject.getPrincipals(User.class).isEmpty());

    // logout twice should be no-op.
    loginContext.logout();
    Assert.assertTrue(subject.getPrincipals(User.class).isEmpty());
  }

   /**
   * This test verifies that logging out a read only subject should fail.
   */
  @Test
  public void logoutReadOnlySubject() throws Exception {
    String clazzName = LoginModuleConfigurationUtils.OS_PRINCIPAL_CLASS_NAME;
    @SuppressWarnings("unchecked")
    Class<? extends Principal> clazz = (Class<? extends Principal>) ClassLoader
        .getSystemClassLoader().loadClass(clazzName);
    Subject subject = new Subject();

    // login, add OS user into subject, and add corresponding Alluxio user into subject
    LoginContext loginContext = new LoginContext("simple", subject, null,
        new LoginModuleConfiguration());
    loginContext.login();

    // verify whether OS user and Alluxio user is added.
    Assert.assertFalse(subject.getPrincipals(clazz).isEmpty());
    Assert.assertFalse(subject.getPrincipals(User.class).isEmpty());

    // logout read only subject should fail.
    subject.setReadOnly();
    mThrown.expect(LoginException.class);
    mThrown.expectMessage("logout Failed: Subject is Readonly");
    loginContext.logout();
    Assert.assertFalse(subject.getPrincipals(clazz).isEmpty());
    Assert.assertFalse(subject.getPrincipals(User.class).isEmpty());
  }

  // TODO(dong): Kerberos login test
}
