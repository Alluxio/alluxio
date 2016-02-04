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

package alluxio.security;

import java.io.IOException;

import org.powermock.reflect.Whitebox;

import alluxio.Constants;
import alluxio.Configuration;

/**
 * Utility methods for the tests using {@link LoginUser}.
 */
public final class LoginUserTestUtils {

  private LoginUserTestUtils() {} // This is a utils class not intended for instantiation

  /**
   * Resets the singleton {@link LoginUser} to null.
   */
  public static void resetLoginUser() {
    Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
  }

  /**
   * Resets the {@link LoginUser} and re-login with new user
   *
   * @param conf the instance of {@link Configuration}
   * @param user the new user
   * @throws IOException if login fails
   */
  public static void resetLoginUser(Configuration conf, String user) throws IOException {
    synchronized (LoginUser.class) {
      resetLoginUser();
      conf.set(Constants.SECURITY_LOGIN_USERNAME, user);
      LoginUser.get(conf);
    }
  }
}
