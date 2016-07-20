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

import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
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

  private String mOwner;
  private String mGroup;
  private Mode mMode;

  /**
   * Constructs an instance of {@link Permission}.
   *
   * @param owner the owner
   * @param group the group
   * @param mode the {@link Mode}
   */
  public Permission(String owner, String group, Mode mode) {
    Preconditions.checkNotNull(owner, PreconditionMessage.PERMISSION_OWNER_IS_NULL);
    Preconditions.checkNotNull(group, PreconditionMessage.PERMISSION_GROUP_IS_NULL);
    Preconditions.checkNotNull(mode, PreconditionMessage.PERMISSION_MODE_IS_NULL);
    mOwner = owner;
    mGroup = group;
    mMode = mode;
  }

  /**
   * Constructs an instance of {@link Permission}. The permission is represented by short.
   *
   * @param owner the owner
   * @param group the group
   * @param mode the {@link Mode} represented by short value
   */
  public Permission(String owner, String group, short mode) {
    this(owner, group, new Mode(mode));
  }

  /**
   * Constructs an instance of {@link Permission} cloned from the given permission.
   *
   * @param p the given permission
   */
  public Permission(Permission p) {
    this(p.getOwner(), p.getGroup(), new Mode(p.getMode()));
  }

  /**
   * @return the owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return the group
   */
  public String getGroup() {
    return mGroup;
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
   * @return this {@link Permission} after umask applied
   */
  public Permission applyFileUMask() {
    mMode = mMode.applyUMask(Mode.getUMask()).applyUMask(FILE_UMASK);
    return this;
  }

  /**
   * Applies the default umask for newly created directories to the mode bits.
   *
   * @return this {@link Permission} after umask applied
   */
  public Permission applyDirectoryUMask() {
    mMode = mMode.applyUMask(Mode.getUMask());
    return this;
  }

  /**
   * Sets the owner based on the thrift transport and updates the group to the primary group of the
   * owner. If authentication is {@link alluxio.security.authentication.AuthType#NOSASL}, this a
   * no-op.
   *
   * @return the {@link Permission} for a file or a directory
   * @throws IOException when getting owner or group information fails
   */
  public Permission setOwnerFromThriftClient() throws IOException {
    if (!SecurityUtils.isAuthenticationEnabled()) {
      // no authentication, no user to set
      return this;
    }
    // get the username through the authentication mechanism
    User user = AuthenticatedClientUser.get();
    if (user == null) {
      throw new IOException(ExceptionMessage.AUTHORIZED_CLIENT_USER_IS_NULL.getMessage());
    }
    mOwner = user.getName();
    mGroup = CommonUtils.getPrimaryGroupName(user.getName());
    return this;
  }

  /**
   * Sets the owner based on the login module and updates the group to the primary group of the
   * owner. If authentication is {@link alluxio.security.authentication.AuthType#NOSASL}, this a
   * no-op.
   *
   * @return the {@link Permission} for a file or a directory
   * @throws IOException when getting owner or group information fails
   */
  public Permission setOwnerFromLoginModule() throws IOException {
    if (!SecurityUtils.isAuthenticationEnabled()) {
      // no authentication, no user to set
      // no authentication, no owner is set
      return this;
    }
    // get the username through the login module
    String user = LoginUser.get().getName();
    mOwner = user;
    mGroup = CommonUtils.getPrimaryGroupName(user);
    return this;
  }

  /**
   * Creates the default {@link Permission} for a file or a directory. Both owner and group are
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
    return Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mOwner, mGroup, mMode);
  }

  @Override
  public String toString() {
    return mOwner + ":" + mGroup + ":" + mMode;
  }
}
