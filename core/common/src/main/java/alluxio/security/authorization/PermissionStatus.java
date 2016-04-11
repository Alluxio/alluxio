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

import javax.annotation.concurrent.ThreadSafe;

/**
 * The permission status for a file or directory.
 */
@ThreadSafe
public final class PermissionStatus {
  private final String mUserName;
  private final String mGroupName;
  private final FileSystemPermission mPermission;

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
   * @see FileSystemPermission#applyUMask(FileSystemPermission)
   */
  public PermissionStatus applyUMask(FileSystemPermission umask) {
    FileSystemPermission newFileSystemPermission = mPermission.applyUMask(umask);
    return new PermissionStatus(mUserName, mGroupName, newFileSystemPermission);
  }

  /**
   * Gets the Directory default {@link PermissionStatus}. Currently the default dir permission is
   * 0777.
   *
   * @return the default {@link PermissionStatus} for directories
   */
  public static PermissionStatus getDirDefault() {
    return new PermissionStatus("", "", new FileSystemPermission(Constants
        .DEFAULT_FS_FULL_PERMISSION));
  }

  // TODO(binfan): remove remote parameter by making two different get methods
  /**
   * Creates the {@link PermissionStatus} for a file or a directory.
   *
   * @param conf the runtime configuration of Alluxio
   * @param remote true if the request is for creating permission from client side, the
   *               username binding into inode will be gotten from {@code AuthenticatedClientUser
   *               .get().getName()}.
   *               If the remote is false, the username binding into inode will be gotten from
   *               {@link alluxio.security.LoginUser}.
   * @return the {@link PermissionStatus} for a file or a directory
   * @throws java.io.IOException when getting login user fails
   */
  public static PermissionStatus get(Configuration conf, boolean remote) throws IOException {
    if (!SecurityUtils.isAuthenticationEnabled(conf)) {
      // no authentication
      return new PermissionStatus("", "", FileSystemPermission.getNoneFsPermission());
    }
    if (remote) {
      // get the username through the authentication mechanism
      User user = AuthenticatedClientUser.get(conf);
      if (user == null) {
        throw new IOException(ExceptionMessage.AUTHORIZED_CLIENT_USER_IS_NULL.getMessage());
      }
      return new PermissionStatus(user.getName(), CommonUtils.getPrimaryGroupName(conf,
          user.getName()), FileSystemPermission.getDefault().applyUMask(conf));
    }

    // get the username through the login module
    String loginUserName = LoginUser.get(conf).getName();
    return new PermissionStatus(loginUserName,
        CommonUtils.getPrimaryGroupName(conf, loginUserName), FileSystemPermission.getDefault()
            .applyUMask(conf));
  }

  @Override
  public String toString() {
    return mUserName + ":" + mGroupName + ":" + mPermission;
  }
}
