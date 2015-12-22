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

import tachyon.exception.ExceptionMessage;
import tachyon.master.file.meta.Inode;
import tachyon.security.authorization.FileSystemAction;
import tachyon.security.authorization.FileSystemPermission;

/**
 * Base class to provide permission check logic.
 */
public final class FileSystemPermissionChecker {
  private static boolean sPermissionCheckEnabled;

  /** The owner of root directory. */
  private static String sFileSystemOwner;

  /** The super group of Tachyon file system. All users in this group have super permission. */
  private static String sFileSystemSuperGroup;

  /**
   * Initializes the permission related property of the whole Tachyon file system.
   *
   * @param permissionCheckEnabled whether permission checking is enabled
   * @param owner the user of root directory, who is seen as the super user
   * @param superGroup the super group of the whole Tachyon file system
   */
  public static synchronized void initializeFileSystem(boolean permissionCheckEnabled, String owner,
      String superGroup) {
    sPermissionCheckEnabled = permissionCheckEnabled;
    sFileSystemOwner = owner;
    sFileSystemSuperGroup = superGroup;
  }

  /**
   * Checks required permission on the last node and basic permission on the whole path.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param action requested {@link FileSystemAction} by user
   * @param inodes all the inodes retrieved by traversing the path
   * @throws IOException if permission checking fails
   */
  public static void checkSelfPermission(String user, List<String> groups, FileSystemAction action,
      List<Inode> inodes) throws IOException {
    checkPermission(user, groups, action, inodes, inodes.size() - 1, -1, false);
  }

  /**
   * Checks required permission on the parent or ancestor of the last node and basic permission
   * on the whole path.
   * Parent means the parent directory of the last node.
   * Ancestor means the last existing (closest) ancestor directory of the last node.
   * If parent exists, ancestor and parent are the same.
   * For example, for a path "/foo/bar/baz", its parent is "/foo/bar".
   * If "/foo/bar" exists, its ancestor is also "/foo/bar".
   * If "/foo/bar" does not exist and "/foo" exists, its ancestor is "/foo".
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param action requested {@link FileSystemAction} by user
   * @param inodes all the inodes retrieved by traversing the path
   * @throws IOException if permission checking fails
   */
  public static void checkParentOrAncestorPermission(String user, List<String> groups,
      FileSystemAction action, List<Inode> inodes) throws IOException {
    int parentOrAncestorIndex = -1;
    if (inodes.size() > 1) {
      for (parentOrAncestorIndex = inodes.size() - 2; inodes.get(parentOrAncestorIndex) == null;
           parentOrAncestorIndex --) {
        // Do nothing. Stop when finding a not null inode from the end of an inodes list.
      }
    }

    checkPermission(user, groups, action, inodes, -1, parentOrAncestorIndex, false);
  }

  /**
   * Checks whether a user is the owner of a path, with basic permission checking succeeding on
   * the whole path.
   *
   * @param user who is checked whether it is the owner
   * @param groups in which user belongs to
   * @param inodes all the inodes retrieved by traversing the path
   * @throws IOException if permission checking fails
   */
  public static void checkOwner(String user, List<String> groups, List<Inode> inodes)
      throws IOException {
    checkPermission(user, groups, null, inodes, -1, -1, true);
  }

  /**
   * This method provides basic permission checking logic.
   * The input is User and its Groups, requested Permission and inodes (traverse the Path).
   * The initialized static attributes will be used in the checking logic to bypass checking.
   * Then User, Group, and Action will be compared to those of inodes.
   * It will return if check passed, and throw exception if check failed.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param action requested {@link FileSystemAction} by user
   * @param inodes all the inodes retrieved by traversing the path
   * @param selfIndex the index of the last inode in the inodes list
   * @param parentOrAncestorIndex the index of the parent or ancestor inode in the inodes list
   * @param isCheckOwner indicates whether to check that the user is the owner of the last inode
   * @throws IOException if permission checking fails
   */
  private static void checkPermission(String user, List<String> groups, FileSystemAction action,
      List<Inode> inodes, int selfIndex, int parentOrAncestorIndex, boolean isCheckOwner)
      throws IOException {
    int size = inodes.size();
    assert size > 0;

    if (!sPermissionCheckEnabled) {
      return;
    }

    if (sFileSystemOwner.equals(user) || groups.contains(sFileSystemSuperGroup)) {
      return;
    }

    // traverses parent path to ensure inodes in it are all executable
    for (int i = 0; i < size - 1; i++) {
      check(user, groups, inodes.get(i), FileSystemAction.EXECUTE);
    }

    // check self permission if needed
    if (selfIndex > -1) {
      check(user, groups, inodes.get(selfIndex), action);
    }

    // check parent or ancestor permission if needed
    if (parentOrAncestorIndex > -1) {
      check(user, groups, inodes.get(parentOrAncestorIndex), action);
    }

    // check owner if needed
    if (isCheckOwner) {
      if (user.equals(inodes.get(size - 1).getUserName())) {
        return;
      }
      throw new IOException(ExceptionMessage.PERMISSION_DENIED.getMessage(toExceptionMessage()));
    }
  }

  private static void check(String user, List<String> groups, Inode inode, FileSystemAction action)
      throws IOException {
    if (inode == null) {
      return;
    }

    short permission = inode.getPermission();
    if (user.equals(inode.getUserName())) {
      if (FileSystemPermission.getUserAction(permission).imply(action)) {
        return;
      }
    } else if (groups.contains(inode.getGroupName())) {
      if (FileSystemPermission.getGroupAction(permission).imply(action)) {
        return;
      }
    } else {
      if (FileSystemPermission.getOtherAction(permission).imply(action)) {
        return;
      }
    }

    throw new IOException(ExceptionMessage.PERMISSION_DENIED.getMessage(toExceptionMessage()));
  }

  private static String toExceptionMessage() {
    // TODO(dong): elaborate message (who, action, resource: failed at inode name, its permission)
    return "";
  }
}
