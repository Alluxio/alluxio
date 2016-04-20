/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The permission status for a file or directory.
 */
@NotThreadSafe
public final class PermissionStatus {
  /** This default umask is used to calculate file permission from directory permission. */
  private static final FileSystemPermission FILE_UMASK =
      new FileSystemPermission(Constants.FILE_DIR_PERMISSION_DIFF);

  private String mUserName;
  private String mGroupName;
  private FileSystemPermission mPermission;

  /**
   * Constructs an instance of {@link PermissionStatus}.
   *
   * @param userName   the user name
   * @param groupName  the group name which the user belongs to
   * @param permission the {@link FileSystemPermission}
   */
  public PermissionStatus(String userName, String groupName, FileSystemPermission permission) {
    mUserName = userName;
    mGroupName = groupName;
    if (permission == null) {
      throw new IllegalArgumentException(ExceptionMessage.PERMISSION_IS_NULL.getMessage());
    }
    mPermission = permission;
  }

  /**
   * Constructs an instance of {@link PermissionStatus}. The permission is represented by short.
   *
   * @param userName   the user name
   * @param groupName  the group name which the user belongs to
   * @param permission the {@link FileSystemPermission} represented by short value
   */
  public PermissionStatus(String userName, String groupName, short permission) {
    this(userName, groupName, new FileSystemPermission(permission));
  }

  /**
   * Constructs an instance of {@link PermissionStatus} cloned from the given permission.
   *
   * @param ps the give permission status
   */
  public PermissionStatus(PermissionStatus ps) {
    this(ps.getUserName(), ps.getGroupName(), new FileSystemPermission(ps.getPermission()));
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
   * @return the {@link FileSystemPermission}
   */
  public FileSystemPermission getPermission() {
    return mPermission;
  }

  /**
   * Applies umask.
   *
   * @param umask the umask to apply
   * @return a new {@link PermissionStatus}
   */
  public PermissionStatus applyUMask(FileSystemPermission umask) {
    mPermission = mPermission.applyUMask(umask);
    return this;
  }

  /**
   * Applies default umask to newly created files.
   *
   * @param conf the runtime configuration of Alluxio
   * @return a new {@link PermissionStatus}
   */
  public PermissionStatus applyFileUMask(Configuration conf) {
    mPermission =
        mPermission.applyUMask(FileSystemPermission.getUMask(conf)).applyUMask(FILE_UMASK);
    return this;
  }

  /**
   * Applies default umask to newly created directories.
   *
   * @param conf the runtime configuration of Alluxio
   * @return a new {@link PermissionStatus}
   */
  public PermissionStatus applyDirectoryUMask(Configuration conf) {
    mPermission =
        mPermission.applyUMask(FileSystemPermission.getUMask(conf));
    return this;
  }

  /**
   * Creates the {@link PermissionStatus} for a file or a directory.
   *
   * @param conf the runtime configuration of Alluxio
   * @return the {@link PermissionStatus} for a file or a directory
   * @throws java.io.IOException when getting login user fails
   */
  public PermissionStatus setUserFromThriftClient(Configuration conf) throws IOException {
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
   * Creates the {@link PermissionStatus} for a file or a directory.
   *
   * @param conf the runtime configuration of Alluxio
   * @return the {@link PermissionStatus} for a file or a directory
   * @throws java.io.IOException when getting login user fails
   */
  public PermissionStatus setUserFromLoginModule(Configuration conf) throws IOException {
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
   * Creates the {@link PermissionStatus} for a file or a directory.
   *
   * @return the {@link PermissionStatus} for a file or a directory
   */
  public static PermissionStatus defaults() {
    // no authentication, every action is permitted
    return new PermissionStatus("", "", FileSystemPermission.getFullFsPermission());
  }

  @Override
  public String toString() {
    return mUserName + ":" + mGroupName + ":" + mPermission;
  }
}
