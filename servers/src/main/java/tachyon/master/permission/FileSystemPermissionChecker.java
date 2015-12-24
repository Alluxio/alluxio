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
import tachyon.exception.AccessControlException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidPathException;
import tachyon.security.authorization.FileSystemAction;
import tachyon.security.authorization.FileSystemPermission;
import tachyon.thrift.FileInfo;
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
   * Checks requested permission and basic permission on the parent of the path.
   * Parent means the parent directory of the path.
   * If parent directory does not exist, treat the closest ancestor directory of the path as
   * its parent and check permission on it.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param action requested {@link FileSystemAction} by user
   * @param path whose parent to check permission on
   * @param fileInfos file infos of all the inodes retrieved by traversing the path
   * @throws AccessControlException if permission checking fails
   */
  public static void checkParentPermission(String user, List<String> groups, FileSystemAction
      action, TachyonURI path, List<FileInfo> fileInfos) throws AccessControlException,
      InvalidPathException {
    String[] pathComponents = PathUtils.getPathComponents(path.getPath());

    if (pathComponents.length == fileInfos.size()) {
      fileInfos.remove(fileInfos.size() - 1);
    }
    checkByFileInfos(user, groups, action, path.getPath(), fileInfos);
  }

  /**
   * Checks requested permission and basic permission on the path.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param action requested {@link FileSystemAction} by user
   * @param path the path to check permission on
   * @param fileInfos file infos of all the inodes retrieved by traversing the path
   * @throws AccessControlException if permission checking fails
   */
  public static void checkPermission(String user, List<String> groups, FileSystemAction action,
      TachyonURI path, List<FileInfo> fileInfos) throws AccessControlException,
      InvalidPathException {
    String[] pathComponents = PathUtils.getPathComponents(path.getPath());

    for (int i = fileInfos.size(); i < pathComponents.length; i++) {
      fileInfos.add(null);
    }
    checkByFileInfos(user, groups, action, path.getPath(), fileInfos);
  }

  /**
   * This method provides basic permission checking logic on a list of fileInfos.
   * The input is User and its Groups, requested Permission and fileInfos (of inodes by traversing
   * the Path).
   * The initialized static attributes will be used in the checking logic to bypass checking.
   * Then User, Group, and Action will be compared to those of inodes.
   * It will return if check passed, and throw exception if check failed.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param action requested {@link FileSystemAction} by user
   * @param path the path to check permission on
   * @param fileInfos file infos of all the inodes retrieved by traversing the path
   * @throws AccessControlException if permission checking fails
   */
  private static void checkByFileInfos(String user, List<String> groups, FileSystemAction action,
      String path, List<FileInfo> fileInfos) throws AccessControlException {
    int size = fileInfos.size();
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
      check(user, groups, fileInfos.get(i), FileSystemAction.EXECUTE, path);
    }

    check(user, groups, fileInfos.get(size - 1), action, path);
  }

  /**
   * This method check requested permission on a given inode, represented by its fileInfo.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param fileInfo whose attributes used for permission check logic
   * @param action requested {@link FileSystemAction} by user
   * @param path the path to check permission on
   * @throws IOException if permission checking fails
   */
  private static void check(String user, List<String> groups, FileInfo fileInfo,
      FileSystemAction action, String path) throws AccessControlException {
    if (fileInfo == null) {
      return;
    }

    short permission = (short) fileInfo.getPermission();

    if (user.equals(fileInfo.getUserName())) {
      if (FileSystemPermission.createUserAction(permission).imply(action)) {
        return;
      }
    }

    if (groups.contains(fileInfo.getGroupName())) {
      if (FileSystemPermission.createGroupAction(permission).imply(action)) {
        return;
      }
    }

    if (FileSystemPermission.createOtherAction(permission).imply(action)) {
      return;
    }

    throw new AccessControlException(
        ExceptionMessage.PERMISSION_DENIED.getMessage(toExceptionMessage(user, action, path,
            fileInfo)));
  }

  private static String toExceptionMessage(String user, FileSystemAction action, String path,
      FileInfo fileInfo) {
    // message format: who, action, resource: failed at where
    StringBuilder stringBuilder = new StringBuilder()
        .append("user=").append(user).append(", ")
        .append("access=").append(action).append(", ")
        .append("path=").append(path).append(": ")
        .append("failed at ")
        .append(fileInfo.getName());
    return stringBuilder.toString();
  }
}
