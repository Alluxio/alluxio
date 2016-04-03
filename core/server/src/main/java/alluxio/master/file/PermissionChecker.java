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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.security.authorization.FileSystemAction;
import alluxio.security.authorization.FileSystemPermission;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Base class to provide permission check logic.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class PermissionChecker {
  private static boolean sPermissionCheckEnabled;

  /** The owner of root directory. */
  private static String sFileSystemOwner;

  /** The super group of Alluxio file system. All users in this group have super permission. */
  private static String sFileSystemSuperGroup;

  /**
   * Initializes the permission related property of the whole Alluxio file system.
   *
   * @param permissionCheckEnabled whether permission checking is enabled
   * @param owner the user of root directory, who is seen as the super user
   * @param superGroup the super group of the whole Alluxio file system
   */
  public static synchronized void initializeFileSystem(boolean permissionCheckEnabled,
      String owner, String superGroup) {
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
   * @param fileInfoList file info list of all the inodes retrieved by traversing the path
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  public static void checkParentPermission(String user, List<String> groups,
      FileSystemAction action, AlluxioURI path, List<FileInfo> fileInfoList)
      throws AccessControlException, InvalidPathException {
    // root "/" has no parent, so return without checking
    if (PathUtils.isRoot(path.getPath())) {
      return;
    }

    String[] pathComponents = PathUtils.getPathComponents(path.getPath());

    // remove the last element if all components of the path exist, since we only check the parent.
    if (pathComponents.length == fileInfoList.size()) {
      fileInfoList.remove(fileInfoList.size() - 1);
    }
    checkByFileInfoList(user, groups, action, path.getPath(), fileInfoList, false);
  }

  /**
   * Checks requested permission and basic permission on the path.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param action requested {@link FileSystemAction} by user
   * @param path the path to check permission on
   * @param fileInfoList file info list of all the inodes retrieved by traversing the path
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  public static void checkPermission(String user, List<String> groups, FileSystemAction action,
      AlluxioURI path, List<FileInfo> fileInfoList) throws AccessControlException,
      InvalidPathException {
    String[] pathComponents = PathUtils.getPathComponents(path.getPath());

    for (int i = fileInfoList.size(); i < pathComponents.length; i++) {
      fileInfoList.add(null);
    }
    checkByFileInfoList(user, groups, action, path.getPath(), fileInfoList, false);
  }

  /**
   * Checks whether the user is the owner of the path.
   *
   * @param user who is verified to be the owner of the path
   * @param groups in which user belongs to
   * @param path the path to check its owner
   * @param fileInfoList file info list of all the inodes retrieved by traversing the path
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  public static void checkOwner(String user, List<String> groups, AlluxioURI path,
      List<FileInfo> fileInfoList) throws AccessControlException, InvalidPathException {
    String[] pathComponents = PathUtils.getPathComponents(path.getPath());

    for (int i = fileInfoList.size(); i < pathComponents.length; i++) {
      fileInfoList.add(null);
    }
    checkByFileInfoList(user, groups, null, path.getPath(), fileInfoList, true);
  }

  /**
   * Checks whether the user is a super user or in super group.
   *
   * @param user who is verified to be the super user
   * @param groups in which user belongs to
   * @throws AccessControlException if the user is not a super user
   */
  public static void checkSuperuser(String user, List<String> groups) throws
      AccessControlException {
    if (sFileSystemOwner.equals(user) || groups.contains(sFileSystemSuperGroup)) {
      return;
    }
    throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED.getMessage(user
        + " is not a super user or in super group"));
  }

  /**
   * This method provides basic permission checking logic on a list of fileInfo.
   * The input is User and its Groups, requested Permission and fileInfo list (of inodes by
   * traversing the Path).
   * The initialized static attributes will be used in the checking logic to bypass checking.
   * Then User, Group, and Action will be compared to those of inodes.
   * It will return if check passed, and throw exception if check failed.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param action requested {@link FileSystemAction} by user
   * @param path the path to check permission on
   * @param fileInfoList file info list of all the inodes retrieved by traversing the path
   * @param checkIsOwner indicates whether to check the user is the owner of the path
   * @throws AccessControlException if permission checking fails
   */
  private static void checkByFileInfoList(String user, List<String> groups, FileSystemAction
      action, String path, List<FileInfo> fileInfoList,
      boolean checkIsOwner) throws AccessControlException {
    int size = fileInfoList.size();
    Preconditions.checkArgument(size > 0,
        PreconditionMessage.EMPTY_FILE_INFO_LIST_FOR_PERMISSION_CHECK);

    if (!sPermissionCheckEnabled) {
      return;
    }

    // bypass checking permission for super user or super group of Alluxio file system.
    if (sFileSystemOwner.equals(user) || groups.contains(sFileSystemSuperGroup)) {
      return;
    }

    // traverses parent path to ensure inodes in it are all executable
    for (int i = 0; i < size - 1; i++) {
      check(user, groups, fileInfoList.get(i), FileSystemAction.EXECUTE, path);
    }

    if (checkIsOwner) {
      FileInfo fileInfo = fileInfoList.get(fileInfoList.size() - 1);
      if (fileInfo == null || user.equals(fileInfo.getUserName())) {
        return;
      }
      throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED.getMessage(
          "user=" + user + " is not the owner of path=" + path));
    } else {
      check(user, groups, fileInfoList.get(fileInfoList.size() - 1), action, path);
    }
  }

  /**
   * This method checks requested permission on a given inode, represented by its fileInfo.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param fileInfo whose attributes used for permission check logic
   * @param action requested {@link FileSystemAction} by user
   * @param path the path to check permission on
   * @throws AccessControlException if permission checking fails
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
        .append(fileInfo.getName().equals("") ? "/" : fileInfo.getName());
    return stringBuilder.toString();
  }
}
