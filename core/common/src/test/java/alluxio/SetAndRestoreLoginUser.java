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
import alluxio.security.LoginUserTestUtils;
import alluxio.security.User;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An AutoCloseable which temporarily modifies process login user when it is
 * constructed and restores the previous login user when it is closed.
 */
@ThreadSafe
public final class SetAndRestoreLoginUser implements AutoCloseable {
  private User mPreviousLoginUser = null;

  /**
   * @param user User name to set
   */
  public SetAndRestoreLoginUser(String user) throws Exception {
    mPreviousLoginUser = LoginUser.get();
    LoginUserTestUtils.resetLoginUser(user);
  }

  @Override
  public void close() throws Exception {
    if (mPreviousLoginUser != null) {
      LoginUserTestUtils.resetLoginUser(mPreviousLoginUser.getName());
    } else {
      LoginUserTestUtils.resetLoginUser();
    }
  }
}
