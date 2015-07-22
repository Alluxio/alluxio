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
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * Unit test for the login module defined in {@link tachyon.security.UserInformation}
 */
public class LoginModuleTest {

  @Before
  public void before() throws Exception {
    UserInformation.reset();
  }

  /**
   * This test verify whether the simple login works in JAAS framework.
   * Simple mode login get the OS user and convert to Tachyon user.
   * @throws Exception
   */
  @Test
  public void simpleLoginTest() throws Exception {
    String clazzName = UserInformation.getOsPrincipalClassName();
    Class<? extends Principal> clazz = (Class<? extends Principal>) ClassLoader
        .getSystemClassLoader().loadClass(clazzName);
    Subject subject = new Subject();

    // login, add OS user into subject, and add corresponding Tachyon user into subject
    LoginContext loginContext = new LoginContext("simple", subject, null,
        new UserInformation.TachyonJaasConfiguration());
    loginContext.login();

    // verify whether OS user and Tachyon user is added.
    Assert.assertFalse(subject.getPrincipals(clazz).isEmpty());
    Assert.assertFalse(subject.getPrincipals(User.class).isEmpty());

    // logout and verify the user is removed
    loginContext.logout();
    Assert.assertTrue(subject.getPrincipals(User.class).isEmpty());
  }

  // TODO: Kerberos login test

  /**
   * Test whether we can get login user with conf in SIMPLE mode.
   * @throws Exception
   */
  @Test
  public void getSimpleLoginUserTest() throws Exception {
    TachyonConf conf = new TachyonConf();

    // set conf to "SIMPLE" and get login user
    conf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");
    UserInformation.setTachyonConf(conf);

    UserInformation loginUser = UserInformation.getTachyonLoginUser();

    Assert.assertFalse(loginUser.getUserName().isEmpty());
  }

  /**
   * Test initializing with conf in NOSASL mode.
   * @throws Exception
   */
  @Test
  public void initConfTest() throws Exception {
    // throw exception when initializing conf with "NOSASL"
    TachyonConf conf = new TachyonConf();
    if (conf.get(Constants.TACHYON_SECURITY_AUTHENTICATION, "").equalsIgnoreCase("NOSASL")) {
      try {
        UserInformation.getTachyonLoginUser();
        Assert.fail();
      } catch (UnsupportedOperationException e) {
        Assert.assertTrue(e.getMessage().contains("not supported in NOSASL mode"));
      }
    } else {
      conf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "NOSASL");
      try {
        UserInformation.setTachyonConf(conf);
        Assert.fail();
      } catch (UnsupportedOperationException e) {
        Assert.assertTrue(e.getMessage().contains("not supported in NOSASL mode"));
      }
    }
  }
}
