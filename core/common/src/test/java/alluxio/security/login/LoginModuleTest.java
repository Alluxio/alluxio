/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
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
