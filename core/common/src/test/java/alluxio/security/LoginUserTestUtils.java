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

import org.powermock.reflect.Whitebox;

/**
 * Utility methods for the tests using {@link LoginUser}.
 */
public final class LoginUserTestUtils {

  private LoginUserTestUtils() {} // prevent instantiation

  /**
   * Resets the singleton {@link LoginUser} to null.
   */
  public static void resetLoginUser() {
    synchronized (LoginUser.class) {
      Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
    }
  }

  /**
   * Resets the {@link LoginUser} and re-login with new user.
   *
   * @param user the new user
   */
  public static void resetLoginUser(String user) {
    synchronized (LoginUser.class) {
      Whitebox.setInternalState(LoginUser.class, "sLoginUser", new User(user));
    }
  }
}
