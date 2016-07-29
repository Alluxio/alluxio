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

import alluxio.security.LoginUser;
import alluxio.security.User;

import org.powermock.reflect.Whitebox;

/**
 * Utility class for using reflection to modify global state for testing purposes.
 */
public final class GlobalStateManagement {

  /**
   * Resets the login user so that the next call to {@link LoginUser#get()} will have to log in
   * again.
   */
  public static void resetLoginUser() throws Exception {
    Whitebox.setInternalState(LoginUser.class, User.class, (User) null);
  }
}
