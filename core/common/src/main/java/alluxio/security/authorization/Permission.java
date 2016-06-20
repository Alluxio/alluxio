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

package alluxio.security.authorization;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.security.LoginUser;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.CommonUtils;
import alluxio.util.SecurityUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The permission for a file or directory.
 */
@NotThreadSafe
public final class Permission {
  /** This default umask is used to calculate file permission from directory permission. */
  private static final Mode FILE_UMASK = new Mode(Constants.FILE_DIR_PERMISSION_DIFF);

  private String mUserName;
  private String mGroupName;
  private Mode mMode;

  /**
   * Constructs an instance of {@link Permission}.
   *
   * @param userName the user name
   * @param groupName the group name which the user belongs to
   * @param mode the {@link Mode}
   */
  public Permission(String userName, String groupName, Mode mode) {
    Preconditions.checkNotNull(mode, ExceptionMessage.MODE_IS_NULL.getMessage());
    mUserName = userName;
    mGroupName = groupName;
    mMode = mode;
  }

  /**
   * Constructs an instance of {@link Permission}. The permission is represented by short.
   *
   * @param userName the user name
   * @param groupName the group name which the user belongs to
   * @param mode the {@link Mode} represented by short value
   */
  public Permission(String userName, String groupName, short mode) {
    this(userName, groupName, new Mode(mode));
  }

  /**
   * Constructs an instance of {@link Permission} cloned from the given permission.
   *
   * @param p the given permission
   */
  public Permission(Permission p) {
    this(p.getUserName(), p.getGroupName(), new Mode(p.getMode()));
  }

  /**
   * @return the user name
   */
  public String getUserName() {
    return mUserName;
  }

  /**
   * @return the group name
   */
  public String getGroupName() {
    return mGroupName;
  }

  /**
   * @return the {@link Mode}
   */
  public Mode getMode() {
    return mMode;
  }

  /**
   * Applies umask to the mode bits.
   *
   * @param umask the umask to apply
   * @return this {@link Permission} after umask applied
   */
  public Permission applyUMask(Mode umask) {
    mMode = mMode.applyUMask(umask);
    return this;
  }

  /**
   * Applies the default umask for newly created files to the mode bits.
   *
   * @param conf the runtime configuration of Alluxio
   * @return this {@link Permission} after umask applied
   */
  public Permission applyFileUMask(Configuration conf) {
    mMode = mMode.applyUMask(Mode.getUMask(conf)).applyUMask(FILE_UMASK);
    return this;
  }

  /**
   * Applies the default umask for newly created directories to the mode bits.
   *
   * @param conf the runtime configuration of Alluxio
   * @return this {@link Permission} after umask applied
   */
  public Permission applyDirectoryUMask(Configuration conf) {
    mMode = mMode.applyUMask(Mode.getUMask(conf));
    return this;
  }

  /**
   * Sets the user based on the thrift transport and updates the group to the primary group of the
   * user. If authentication is {@link alluxio.security.authentication.AuthType#NOSASL}, this a
   * no-op.
   *
   * @param conf the runtime configuration of Alluxio
   * @return the {@link Permission} for a file or a directory
   * @throws IOException when getting login user fails
   */
  public Permission setUserFromThriftClient(Configuration conf) throws IOException {
    if (!SecurityUtils.isAuthenticationEnabled(conf)) {
      // no authentication, no user to set
      return this;
    }
    // get the username through the authentication mechanism
    User user = AuthenticatedClientUser.get(conf);
    if (user == null) {
      throw new IOException(ExceptionMessage.AUTHORIZED_CLIENT_USER_IS_NULL.getMessage());
    }
    mUserName = user.getName();
    mGroupName = CommonUtils.getPrimaryGroupName(conf, user.getName());
    return this;
  }

  /**
   * Sets the user based on the login module and updates the group to the primary group of the user.
   * If authentication is {@link alluxio.security.authentication.AuthType#NOSASL}, this a no-op.
   *
   * @param conf the runtime configuration of Alluxio
   * @return the {@link Permission} for a file or a directory
   * @throws IOException when getting login user fails
   */
  public Permission setUserFromLoginModule(Configuration conf) throws IOException {
    if (!SecurityUtils.isAuthenticationEnabled(conf)) {
      // no authentication, no user to set
      return this;
    }
    // get the username through the login module
    String loginUserName = LoginUser.get(conf).getName();
    mUserName = loginUserName;
    mGroupName = CommonUtils.getPrimaryGroupName(conf, loginUserName);
    return this;
  }

  /**
   * Creates the default {@link Permission} for a file or a directory. Both user and group are
   * empty and the "full access" mode is used.
   *
   * @return the {@link Permission} for a file or a directory
   */
  public static Permission defaults() {
    // no authentication, every action is permitted
    return new Permission("", "", Mode.createFullAccess());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Permission)) {
      return false;
    }
    Permission that = (Permission) o;
    return Objects.equal(mUserName, that.mUserName)
        && Objects.equal(mGroupName, that.mGroupName)
        && Objects.equal(mMode, that.mMode);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mUserName, mGroupName, mMode);
  }

  @Override
  public String toString() {
    return mUserName + ":" + mGroupName + ":" + mMode;
  }
}
