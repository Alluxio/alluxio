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

package alluxio.security;

import alluxio.Configuration;
import alluxio.Constants;

import org.powermock.reflect.Whitebox;

import java.io.IOException;

/**
 * Utility methods for the tests using {@link LoginUser}.
 */
public final class LoginUserTestUtils {

  private LoginUserTestUtils() {} // prevent instantiation

  /**
   * Resets the singleton {@link LoginUser} to null.
   */
  public static void resetLoginUser() {
    Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
  }

  /**
   * Resets the {@link LoginUser} and re-login with new user.
   *
   * @param user the new user
   * @throws IOException if login fails
   */
  public static void resetLoginUser(String user) throws IOException {
    synchronized (LoginUser.class) {
      resetLoginUser();
      Configuration.set(Constants.SECURITY_LOGIN_USERNAME, user);
      LoginUser.get();
    }
  }
}
