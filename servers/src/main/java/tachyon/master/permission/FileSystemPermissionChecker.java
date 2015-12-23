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

import tachyon.TachyonURI;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidPathException;
import tachyon.master.file.meta.Inode;
import tachyon.master.file.meta.InodeTree;
import tachyon.security.authorization.FileSystemAction;
import tachyon.security.authorization.FileSystemPermission;
import tachyon.util.io.PathUtils;

/**
 * Base class to provide permission check logic.
 */
public final class FileSystemPermissionChecker {
  private static boolean sPermissionCheckEnabled;

  /** The owner of root directory. */
  private static String sFileSystemOwner;

  /** The super group of Tachyon file system. All users in this group have super permission. */
  private static String sFileSystemSuperGroup;

  /** Used to traverse a path and get all inodes on the path. */
  private static InodeTree sInodeTree;

  /**
   * Initializes the permission related property of the whole Tachyon file system.
   *
   * @param permissionCheckEnabled whether permission checking is enabled
   * @param owner the user of root directory, who is seen as the super user
   * @param superGroup the super group of the whole Tachyon file system
   */
  public static synchronized void initializeFileSystem(boolean permissionCheckEnabled, String owner,
      String superGroup, InodeTree inodeTree) {
    sPermissionCheckEnabled = permissionCheckEnabled;
    sFileSystemOwner = owner;
    sFileSystemSuperGroup = superGroup;
    sInodeTree = inodeTree;
  }

  /**
   * Checks requested permission and basic permission on the parent of the path.
   * Parent means the parent directory of the path.
   * If parent directory does not exist, treat the closest ancestor directory of the path as
   * its parent and check permission on it.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param action requested {@link FileSystemAction} by user
   * @param path whose parent to check permission on
   * @throws IOException if permission checking fails
   */
  public static void checkParentPermission(String user, List<String> groups, FileSystemAction
      action, TachyonURI path) throws IOException, InvalidPathException {
    String[] pathComponents = PathUtils.getPathComponents(path.getPath());
    List<Inode> inodes = sInodeTree.collectInodes(path);

    if (pathComponents.length == inodes.size()) {
      inodes.remove(inodes.size() - 1);
    }
    checkOnInodes(user, groups, action, inodes);
  }

  /**
   * Checks requested permission and basic permission on the path.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param action requested {@link FileSystemAction} by user
   * @param path the path to check permission on
   * @throws IOException if permission checking fails
   */
  public static void checkPermission(String user, List<String> groups, FileSystemAction action,
      TachyonURI path) throws IOException, InvalidPathException {
    String[] pathComponents = PathUtils.getPathComponents(path.getPath());
    List<Inode> inodes = sInodeTree.collectInodes(path);
    for (int i = inodes.size(); i < pathComponents.length; i++) {
      inodes.add(null);
    }
    checkOnInodes(user, groups, action, inodes);
  }

  /**
   * This method provides basic permission checking logic on a list of inodes.
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
  private static void checkOnInodes(String user, List<String> groups, FileSystemAction action,
      List<Inode> inodes) throws IOException {
    int size = inodes.size();
    assert size > 0;

    if (!sPermissionCheckEnabled) {
      return;
    }

    // bypass checking permission for super user or super group of Tachyon file system.
    if (sFileSystemOwner.equals(user) || groups.contains(sFileSystemSuperGroup)) {
      return;
    }

    // traverses parent path to ensure inodes in it are all executable
    for (int i = 0; i < size - 1; i++) {
      check(user, groups, inodes.get(i), FileSystemAction.EXECUTE);
    }

    check(user, groups, inodes.get(size - 1), action);
  }

  /**
   * This method check requested permission on a given inode.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param inode on which apply permission check logic
   * @param action requested {@link FileSystemAction} by user
   * @throws IOException if permission checking fails
   */
  private static void check(String user, List<String> groups, Inode inode, FileSystemAction action)
      throws IOException {
    if (inode == null) {
      return;
    }

    short permission = inode.getPermission();

    if (user.equals(inode.getUserName())) {
      if (FileSystemPermission.createUserAction(permission).imply(action)) {
        return;
      }
    }

    if (groups.contains(inode.getGroupName())) {
      if (FileSystemPermission.createGroupAction(permission).imply(action)) {
        return;
      }
    }

    if (FileSystemPermission.createOtherAction(permission).imply(action)) {
      return;
    }

    throw new IOException(ExceptionMessage.PERMISSION_DENIED.getMessage(toExceptionMessage()));
  }

  private static String toExceptionMessage() {
    // TODO(dong): elaborate message (who, action, resource: failed at inode name, its permission)
    return "";
  }
}
