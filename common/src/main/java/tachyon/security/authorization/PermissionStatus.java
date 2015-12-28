/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.security.authorization;

import java.io.IOException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.security.LoginUser;
import tachyon.security.User;
import tachyon.security.authentication.AuthType;
import tachyon.security.authentication.PlainSaslServer;

/**
 * The permission status for a file or directory.
 */
public final class PermissionStatus {
  private String mUserName;
  private String mGroupName;
  private FileSystemPermission mPermission;

  /**
   * Constructs an instance of {@link PermissionStatus}
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
        .DEFAULT_TFS_FULL_PERMISSION));
  }

  /**
   * Creates the {@link PermissionStatus} for a file or a directory.
   *
   * @param conf the runtime configuration of Tachyon
   * @param remote true if the request is for creating permission from client side, the
   *               username binding into inode will be gotten from {@code AuthorizedClientUser
   *               .get().getName()}.
   *               If the remote is false, the username binding into inode will be gotten from
   *               {@link tachyon.security.LoginUser}.
   * @return the {@link PermissionStatus} for a file or a directory
   * @throws java.io.IOException when getting login user fails
   */
  public static PermissionStatus get(TachyonConf conf, boolean remote) throws IOException {
    AuthType authType = conf.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    if (authType == AuthType.NOSASL) {
      // no authentication
      return new PermissionStatus("", "", FileSystemPermission.getNoneFsPermission());
    }
    if (remote) {
      // get the username through the authentication mechanism
      User user = PlainSaslServer.AuthorizedClientUser.get();
      if (user == null) {
        throw new IOException(ExceptionMessage.AUTHORIZED_CLIENT_USER_IS_NULL.getMessage());
      }
      return new PermissionStatus(user.getName(),
          "", // TODO(dong) group permission binding into Inode
          FileSystemPermission.getDefault().applyUMask(conf));
    }

    // get the username through the login module
    String loginUserName = LoginUser.get(conf).getName();
    return new PermissionStatus(loginUserName,
        "", // TODO(dong) group permission binding into Inode
        FileSystemPermission.getDefault().applyUMask(conf));
  }

  @Override
  public String toString() {
    return mUserName + ":" + mGroupName + ":" + mPermission;
  }
}
