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

package tachyon.master.permission;

import java.io.IOException;
import java.util.List;

import tachyon.master.file.meta.Inode;
import tachyon.security.User;
import tachyon.security.authorization.FileSystemAction;

/**
 * Base class to provide permission check logic.
 */
public final class FileSystemPermissionChecker {
  private static boolean sPermissionCheckEnabled;

  /** The owner of root directory. */
  private static String sFileSystemOwner;

  /** The super group of Tachyon file system. All users in this group has super permission. */
  private static String sFileSystemSuperGroup;

  /**
   * Initializes the permission related property of the whole Tachyon file system.
   *
   * @param permissionCheckEnabled whether permission checking is enabled
   * @param owner the user of root directory, who is seen as the super user
   * @param superGroup the super group of the whole Tachyon file system
   */
  public static void initializeFileSystem(boolean permissionCheckEnabled, String owner,
      String superGroup) {
    sPermissionCheckEnabled = permissionCheckEnabled;
    sFileSystemOwner = owner;
    sFileSystemSuperGroup = superGroup;
  }

  /**
   * This method checks access permission.
   * The input is User and its Groups, requested Permission and inodes (traverse the Path).
   * The initialized static attributes will be used in the checking logic to bypass checking.
   * Then User, Group, and Action will be compared to those of inodes.
   * It will return if check passed, and throw exception if check failed.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param action requested {@link FileSystemAction} by user
   * @param inodes all the inodes retrieved by traversing the path
   * @throws IOException if permission checking fails
   */
  public static void checkPermission(User user, List<String> groups, FileSystemAction action,
      List<Inode> inodes) throws IOException {
    if (!sPermissionCheckEnabled) {
      return;
    }

    if (sFileSystemOwner.equals(user.getName()) || groups.contains(sFileSystemSuperGroup)) {
      return;
    }
    // TODO(dong): Implement TACHYON-1416
  }
}
