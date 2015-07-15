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

package tachyon.security;

import java.security.Principal;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the login module defined in {@link tachyon.security.UserInformation}
 */
public class LoginModuleTest {

  /**
   * This test verify whether the OS login module works in JAAS framework.
   * @throws Exception
   */
  @Test
  public void osLoginModuleTest() throws Exception {
    String clazzName = UserInformation.getOsPrincipalClassName();
    Class<? extends Principal> clazz = (Class<? extends Principal>) ClassLoader
        .getSystemClassLoader().loadClass(clazzName);
    Subject subject = new Subject();

    // login and add OS user into subject
    LoginContext loginContext = new LoginContext("simple", subject, null,
        new UserInformation.TachyonJaasConfiguration());
    loginContext.login();

    // verify whether OS user is fetched
    Assert.assertFalse(subject.getPrincipals(clazz).isEmpty());
  }

  /**
   * This test verify whether the Tachyon login module works in JAAS framework
   * @throws Exception
   */
  @Test
  public void tachyonLoginModuleTest() throws Exception {
    // login, find a OS user, and add corresponding Tachyon user into subject
    Subject subject = new Subject();
    LoginContext loginContext = new LoginContext("simple", subject, null,
        new UserInformation.TachyonJaasConfiguration());
    loginContext.login();

    // verify whether Tachyon user is added
    Assert.assertFalse(subject.getPrincipals(User.class).isEmpty());

    // logout and verify the user is removed
    loginContext.logout();
    Assert.assertTrue(subject.getPrincipals(User.class).isEmpty());
  }
}
