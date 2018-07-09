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
import alluxio.security.authentication.AuthenticatedClientUser;

import java.io.Closeable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A resource for changing the Alluxio client user during a test.
 */
@NotThreadSafe
public final class UserResource implements Closeable {
  User mPreviousLogin;
  User mPreviousAuthenticated;

  public UserResource(String user) throws Exception {
    mPreviousLogin = LoginUser.get();
    mPreviousAuthenticated = AuthenticatedClientUser.get();

    LoginUserTestUtils.resetLoginUser(user);
    AuthenticatedClientUser.set(user);
  }

  @Override
  public void close() {
    LoginUserTestUtils.resetLoginUser(mPreviousLogin.getName());
    AuthenticatedClientUser.set(mPreviousAuthenticated.getName());
  }
}
