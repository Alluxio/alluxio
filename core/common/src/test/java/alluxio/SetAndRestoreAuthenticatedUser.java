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

import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;

/**
 * An AutoCloseable which temporarily modifies authenticated user on server side when it is
 * constructed and restores the authenticated user when it is closed.
 */
public final class SetAndRestoreAuthenticatedUser implements AutoCloseable {
  private final User mPreviousUser;

  /**
   * @param user User name to set
   */
  public SetAndRestoreAuthenticatedUser(String user) throws Exception {
    mPreviousUser = AuthenticatedClientUser.get();
    AuthenticatedClientUser.set(user);
  }

  @Override
  public void close() throws Exception {
    if (mPreviousUser == null) {
      AuthenticatedClientUser.remove();
    } else {
      AuthenticatedClientUser.set(mPreviousUser.getName());
    }
  }
}
