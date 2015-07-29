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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * Unit test for {@link tachyon.security.LoginUser}
 */
public class LoginUserTest {

  private static Method sGet;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  // Use reflection to get the private static method get(conf) in LoginUser and set it accessible.
  @BeforeClass
  public static void beforeClass() throws Exception {
    sGet = LoginUser.class.getDeclaredMethod("get", TachyonConf.class);
    sGet.setAccessible(true);
  }

  // User reflection to reset the private static member sLoginUser in LoginUser.
  @Before
  public void before() throws Exception {
    Field field = LoginUser.class.getDeclaredField("sLoginUser");
    field.setAccessible(true);
    field.set(null, null);
  }

  /**
   * Test whether we can get login user with conf in SIMPLE mode.
   * @throws Exception
   */
  @Test
  public void getSimpleLoginUserTest() throws Exception {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "SIMPLE");

    User loginUser = (User) sGet.invoke(null, conf);

    Assert.assertNotNull(loginUser);
    Assert.assertFalse(loginUser.getName().isEmpty());
  }

  // TODO: getKerberosLoginUserTest()

  /**
   * Test whether we can get exception when getting a login user in non-security mode
   * @throws Exception
   */
  @Test
  public void securityEnabledTest() throws Throwable {
    // TODO: add Kerberos in the white list when it is supported.
    // throw exception when AuthType is not "SIMPLE"
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.TACHYON_SECURITY_AUTHENTICATION, "NOSASL");

    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("User is only supported in SIMPLE mode");

    // The InvocationTargetException wraps the wanted UnsupportedOperationException.
    try {
      sGet.invoke(null, conf);
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
  }
}
