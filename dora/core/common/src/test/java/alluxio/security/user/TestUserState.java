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

package alluxio.security.user;

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.User;

import javax.security.auth.Subject;

/**
 * UserState for testing only.
 */
public class TestUserState extends BaseUserState {
  /**
   * Creates a test UserState. This will create a fresh subject to use.
   *
   * @param user the test user name
   * @param conf the Alluxio configuration
   */
  public TestUserState(String user, AlluxioConfiguration conf) {
    this (new Subject(), user, conf);
  }

  /**
   * Creates a test UserState.
   *
   * @param subject the test subject
   * @param user the test user name
   * @param conf the Alluxio configuration
   */
  public TestUserState(Subject subject, String user, AlluxioConfiguration conf) {
    super(subject, conf);
    mUser = new User(user);
    mSubject.getPrincipals().add(mUser);
  }

  @Override
  public User login() throws UnauthenticatedException {
    return mUser;
  }
}
