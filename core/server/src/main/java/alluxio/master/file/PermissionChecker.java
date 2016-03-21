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
import alluxio.Constants;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.master.MasterContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.FileSystemAction;
import alluxio.security.authorization.FileSystemPermission;
import alluxio.security.group.GroupMappingService;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Base class to provide permission check logic.
 */
@NotThreadSafe
public final class PermissionChecker {
  /** The file system inode structure. */
  private final InodeTree mInodeTree;

  /** Whether the permission check is enabled. */
  private final boolean mPermissionCheckEnabled;

  /** The owner of root directory. */
  private final String mFileSystemOwner;

  /** The super group of Alluxio file system. All users in this group have super permission. */
  private final String mFileSystemSuperGroup;

  /** This provides user groups mapping service. */
  private final GroupMappingService mGroupMappingService;

  /**
   * Constructs a permission checker instance for Alluxio file system.
   *
   * @param inodeTree inode tree of the file system master
   */
  public PermissionChecker(InodeTree inodeTree) {
    mInodeTree = Preconditions.checkNotNull(inodeTree);
    mPermissionCheckEnabled =
        MasterContext.getConf().getBoolean(Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED);
    mFileSystemOwner = mInodeTree.getRootUserName();
    mFileSystemSuperGroup =
        MasterContext.getConf().get(Constants.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP);
    mGroupMappingService =
        GroupMappingService.Factory.getUserToGroupsMappingService(MasterContext.getConf());
  }

  /**
   * Checks whether a user has permission to perform a specific action on the parent of the given
   * path. This check will pass if the path is invalid, or path has no parent (e.g., root).
   *
   * @param action requested {@link FileSystemAction} by user
   * @param path the path to check permission on
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  @GuardedBy("mInodeTree")
  public void checkParentPermission(FileSystemAction action, AlluxioURI path)
      throws AccessControlException, InvalidPathException {
    if (!mPermissionCheckEnabled) {
      return;
    }

    // root "/" has no parent, so return without checking
    if (PathUtils.isRoot(path.getPath())) {
      return;
    }

    // collects inodes info on the path
    List<Inode<?>> inodeList = mInodeTree.collectInodes(path);

    // collects user and groups
    String user = getClientUser();
    List<String> groups = getGroups(user);

    // perform permission check
    String[] pathComponents = PathUtils.getPathComponents(path.getPath());

    // Checks requested permission and basic permission on the parent of the path.
    // If parent directory does not exist, treats the closest ancestor directory of the path as
    // its parent and check permission on it.

    // remove the last element if all components of the path exist, since we only check the parent.
    if (pathComponents.length == inodeList.size()) {
      inodeList.remove(inodeList.size() - 1);
    }
    checkInodeList(user, groups, action, path.getPath(), inodeList, false);
  }

  /**
   * Checks whether a user has permission to perform a specific action on a path. This check will
   * pass if the path is invalid.
   *
   * @param action requested {@link FileSystemAction} by user
   * @param path the path to check permission on
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  @GuardedBy("mInodeTree")
  public void checkPermission(FileSystemAction action, AlluxioURI path)
      throws AccessControlException, InvalidPathException {
    if (!mPermissionCheckEnabled) {
      return;
    }

    // collects inodes info on the path
    List<Inode<?>> inodeList = mInodeTree.collectInodes(path);

    // collects user and groups
    String user = getClientUser();
    List<String> groups = getGroups(user);

    // Checks requested permission and basic permission on the path.
    String[] pathComponents = PathUtils.getPathComponents(path.getPath());
    for (int i = inodeList.size(); i < pathComponents.length; i++) {
      inodeList.add(null);
    }
    checkInodeList(user, groups, action, path.getPath(), inodeList, false);
  }

  /**
   * Checks whether a user has permission to edit the attribute of a given path.
   *
   * @param path the path to check permission on
   * @param superuserRequired indicates whether it requires to be the superuser
   * @param ownerRequired indicates whether it requires to be the owner of this path
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  @GuardedBy("mInodeTree")
  public void checkSetAttributePermission(AlluxioURI path, boolean superuserRequired,
      boolean ownerRequired) throws AccessControlException, InvalidPathException {
    if (!mPermissionCheckEnabled) {
      return;
    }

    // For chown, superuser is required
    if (superuserRequired) {
      checkSuperuser();
    }
    // For chgrp or chmod, owner is required
    if (ownerRequired) {
      checkOwner(path);
    }
    checkPermission(FileSystemAction.WRITE, path);
  }

  /**
   * @return the client user
   * @throws AccessControlException if the client user information cannot be accessed
   */
  private String getClientUser() throws AccessControlException {
    try {
      User authorizedUser = AuthenticatedClientUser.get(MasterContext.getConf());
      if (authorizedUser == null) {
        throw new AccessControlException(
            ExceptionMessage.AUTHORIZED_CLIENT_USER_IS_NULL.getMessage());
      }
      return authorizedUser.getName();
    } catch (IOException e) {
      throw new AccessControlException(e.getMessage());
    }
  }

  /**
   * @param user the user to get groups for
   * @return the groups for the given user
   * @throws AccessControlException if the group service information cannot be accessed
   */
  private List<String> getGroups(String user) throws AccessControlException {
    try {
      return mGroupMappingService.getGroups(user);
    } catch (IOException e) {
      throw new AccessControlException(
          ExceptionMessage.PERMISSION_DENIED.getMessage(e.getMessage()));
    }
  }

  /**
   * Checks whether the client user is the owner of the path.
   *
   * @param path to be checked on
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  @GuardedBy("mInodeTree")
  private void checkOwner(AlluxioURI path) throws AccessControlException, InvalidPathException {
    // collects inodes info on the path
    List<Inode<?>> inodeList = mInodeTree.collectInodes(path);

    // collects user and groups
    String user = getClientUser();
    List<String> groups = getGroups(user);

    // checks the owner
    String[] pathComponents = PathUtils.getPathComponents(path.getPath());

    for (int i = inodeList.size(); i < pathComponents.length; i++) {
      inodeList.add(null);
    }
    checkInodeList(user, groups, null, path.getPath(), inodeList, true);
  }

  /**
   * Checks whether the user is a super user or in super group.
   *
   * @throws AccessControlException if the user is not a super user
   */
  private void checkSuperuser() throws AccessControlException {
    // collects user and groups
    String user = getClientUser();
    List<String> groups = getGroups(user);

    if (mFileSystemOwner.equals(user) || groups.contains(mFileSystemSuperGroup)) {
      return;
    }
    throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED
        .getMessage(user + " is not a super user or in super group"));
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
   * @param inodeList file info list of all the inodes retrieved by traversing the path
   * @param checkIsOwner indicates whether to check the user is the owner of the path
   * @throws AccessControlException if permission checking fails
   */
  private void checkInodeList(String user, List<String> groups, FileSystemAction action,
      String path, List<Inode<?>> inodeList, boolean checkIsOwner)
      throws AccessControlException {
    int size = inodeList.size();
    Preconditions
        .checkArgument(size > 0, PreconditionMessage.EMPTY_FILE_INFO_LIST_FOR_PERMISSION_CHECK);

    // bypass checking permission for super user or super group of Alluxio file system.
    if (mFileSystemOwner.equals(user) || groups.contains(mFileSystemSuperGroup)) {
      return;
    }

    // traverses parent path to ensure inodes in it are all executable
    for (int i = 0; i < size - 1; i++) {
      checkInode(user, groups, inodeList.get(i), FileSystemAction.EXECUTE, path);
    }

    Inode inode = inodeList.get(inodeList.size() - 1);
    if (checkIsOwner) {
      if (inode == null || user.equals(inode.getUserName())) {
        return;
      }
      throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED
          .getMessage("user=" + user + " is not the owner of path=" + path));
    }
    checkInode(user, groups, inode, action, path);
  }

  /**
   * This method checks requested permission on a given inode, represented by its fileInfo.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param inode whose attributes used for permission check logic
   * @param action requested {@link FileSystemAction} by user
   * @param path the path to check permission on
   * @throws AccessControlException if permission checking fails
   */
  private void checkInode(String user, List<String> groups, Inode inode,
      FileSystemAction action, String path) throws AccessControlException {
    if (inode == null) {
      return;
    }

    short permission = inode.getPermission();

    if (user.equals(inode.getUserName()) && FileSystemPermission.createUserAction(permission)
        .imply(action)) {
      return;
    }

    if (groups.contains(inode.getGroupName()) && FileSystemPermission
        .createGroupAction(permission).imply(action)) {
      return;
    }

    if (FileSystemPermission.createOtherAction(permission).imply(action)) {
      return;
    }

    throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED
        .getMessage(toExceptionMessage(user, action, path, inode)));
  }

  private static String toExceptionMessage(String user, FileSystemAction action, String path,
      Inode inode) {
    // message format: who, action, resource: failed at where
    StringBuilder stringBuilder =
        new StringBuilder().append("user=").append(user).append(", ").append("access=")
            .append(action).append(", ").append("path=").append(path).append(": ")
            .append("failed at ").append(inode.getName().equals("") ? "/" : inode.getName());
    return stringBuilder.toString();
  }
}
