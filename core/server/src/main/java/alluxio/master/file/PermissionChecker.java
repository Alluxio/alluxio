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

package alluxio.master.file;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Base class to provide permission check logic.
 */
// TODO(peis): Migrate this class to a set of static functions.
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class PermissionChecker {
  /** The file system inode structure. */
  private final InodeTree mInodeTree;

  /** Whether the permission check is enabled. */
  private final boolean mPermissionCheckEnabled;

  /** The super group of Alluxio file system. All users in this group have super permission. */
  private final String mFileSystemSuperGroup;

  /**
   * Constructs a {@link PermissionChecker} instance for Alluxio file system.
   *
   * @param inodeTree inode tree of the file system master
   */
  public PermissionChecker(InodeTree inodeTree) {
    mInodeTree = Preconditions.checkNotNull(inodeTree);
    mPermissionCheckEnabled =
        Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED);
    mFileSystemSuperGroup =
        Configuration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP);
  }

  /**
   * Checks whether a user has permission to perform a specific action on the parent of the given
   * path; if parent directory does not exist, treats the closest ancestor directory of the path as
   * its parent and checks permission on it. This check will pass if the path is invalid, or path
   * has no parent (e.g., root).
   *
   * @param bits bits that capture the action {@link Mode.Bits} by user
   * @param inodePath the path to check permission on
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  public void checkParentPermission(Mode.Bits bits, LockedInodePath inodePath)
      throws AccessControlException, InvalidPathException {
    if (!mPermissionCheckEnabled) {
      return;
    }

    // root "/" has no parent, so return without checking
    if (PathUtils.isRoot(inodePath.getUri().getPath())) {
      return;
    }

    // collects existing inodes info on the path. Note that, not all the components of the path have
    // corresponding inodes.
    List<Inode<?>> inodeList = inodePath.getInodeList();

    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser();
    List<String> groups = getGroups(user);

    // remove the last element if all components of the path exist, since we only check the parent.
    if (inodePath.fullPathExists()) {
      inodeList.remove(inodeList.size() - 1);
    }
    checkInodeList(user, groups, bits, inodePath.getUri().getPath(), inodeList, false);
  }

  /**
   * Checks whether a user has permission to perform a specific action on a path. This check will
   * pass if the path is invalid.
   *
   * @param bits bits that capture the action {@link Mode.Bits} by user
   * @param inodePath the path to check permission on
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  public void checkPermission(Mode.Bits bits, LockedInodePath inodePath)
      throws AccessControlException, InvalidPathException {
    if (!mPermissionCheckEnabled) {
      return;
    }

    // collects inodes info on the path
    List<Inode<?>> inodeList = inodePath.getInodeList();

    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser();
    List<String> groups = getGroups(user);

    checkInodeList(user, groups, bits, inodePath.getUri().getPath(), inodeList, false);
  }

  /**
   * Gets the permission to access inodePath for the current client user.
   *
   * @param inodePath the inode path
   * @return the permission
   */
  public Mode.Bits getPermission(LockedInodePath inodePath) {
    if (!mPermissionCheckEnabled) {
      return Mode.Bits.NONE;
    }
    // collects inodes info on the path
    List<Inode<?>> inodeList = inodePath.getInodeList();

    // collects user and groups
    try {
      String user = AuthenticatedClientUser.getClientUser();
      List<String> groups = getGroups(user);
      return getPermissionInternal(user, groups, inodePath.getUri().getPath(), inodeList);
    } catch (AccessControlException e) {
      return Mode.Bits.NONE;
    }
  }

  /**
   * Checks whether a user has permission to edit the attribute of a given path.
   *
   * @param inodePath the path to check permission on
   * @param superuserRequired indicates whether it requires to be the superuser
   * @param ownerRequired indicates whether it requires to be the owner of this path
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  public void checkSetAttributePermission(LockedInodePath inodePath, boolean superuserRequired,
      boolean ownerRequired) throws AccessControlException, InvalidPathException {
    if (!mPermissionCheckEnabled) {
      return;
    }

    // For chown, superuser is required
    if (superuserRequired) {
      checkSuperUser();
    }
    // For chgrp or chmod, owner or superuser (supergroup) is required
    if (ownerRequired) {
      checkOwner(inodePath);
    }
    checkPermission(Mode.Bits.WRITE, inodePath);
  }

  /**
   * @param user the user to get groups for
   * @return the groups for the given user
   * @throws AccessControlException if the group service information cannot be accessed
   */
  private List<String> getGroups(String user) throws AccessControlException {
    try {
      return CommonUtils.getGroups(user);
    } catch (IOException e) {
      throw new AccessControlException(
          ExceptionMessage.PERMISSION_DENIED.getMessage(e.getMessage()));
    }
  }

  /**
   * Checks whether the client user is the owner of the path.
   *
   * @param inodePath path to be checked on
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  private void checkOwner(LockedInodePath inodePath)
      throws AccessControlException, InvalidPathException {
    // collects inodes info on the path
    List<Inode<?>> inodeList = inodePath.getInodeList();

    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser();
    List<String> groups = getGroups(user);

    if (isPrivilegedUser(user, groups)) {
      return;
    }

    checkInodeList(user, groups, null, inodePath.getUri().getPath(), inodeList, true);
  }

  /**
   * Checks whether the user is a super user or in super group.
   *
   * @throws AccessControlException if the user is not a super user
   */
  private void checkSuperUser() throws AccessControlException {
    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser();
    List<String> groups = getGroups(user);
    if (!isPrivilegedUser(user, groups)) {
      throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED
          .getMessage(user + " is not a super user or in super group"));
    }
  }

  /**
   * This method provides basic permission checking logic on a list of inodes. The input includes
   * user and its group, requested action and inode list (by traversing the path). Then user,
   * group, and the requested action will be evaluated on each of the inodes. It will return if
   * check passed, and throw exception if check failed.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param bits bits that capture the action {@link Mode.Bits} by user
   * @param path the path to check permission on
   * @param inodeList file info list of all the inodes retrieved by traversing the path
   * @param checkIsOwner indicates whether to check the user is the owner of the path
   * @throws AccessControlException if permission checking fails
   */
  private void checkInodeList(String user, List<String> groups, Mode.Bits bits,
      String path, List<Inode<?>> inodeList, boolean checkIsOwner) throws AccessControlException {
    int size = inodeList.size();
    Preconditions
        .checkArgument(size > 0, PreconditionMessage.EMPTY_FILE_INFO_LIST_FOR_PERMISSION_CHECK);

    // bypass checking permission for super user or super group of Alluxio file system.
    if (isPrivilegedUser(user, groups)) {
      return;
    }

    // traverses from root to the parent dir to all inodes included by this path are executable
    for (int i = 0; i < size - 1; i++) {
      checkInode(user, groups, inodeList.get(i), Mode.Bits.EXECUTE, path);
    }

    Inode inode = inodeList.get(inodeList.size() - 1);
    if (checkIsOwner) {
      if (inode == null || user.equals(inode.getOwner())) {
        return;
      }
      throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED
          .getMessage("user=" + user + " is not the owner of path=" + path));
    }
    checkInode(user, groups, inode, bits, path);
  }

  /**
   * This method checks requested permission on a given inode, represented by its fileInfo.
   *
   * @param user who requests access permission
   * @param groups in which user belongs to
   * @param inode whose attributes used for permission check logic
   * @param bits requested {@link Mode.Bits} by user
   * @param path the path to check permission on
   * @throws AccessControlException if permission checking fails
   */
  private void checkInode(String user, List<String> groups, Inode<?> inode, Mode.Bits bits,
      String path) throws AccessControlException {
    if (inode == null) {
      return;
    }

    short permission = inode.getMode();

    if (user.equals(inode.getOwner()) && Mode.extractOwnerBits(permission).imply(bits)) {
      return;
    }

    if (groups.contains(inode.getGroup()) && Mode.extractGroupBits(permission).imply(bits)) {
      return;
    }

    if (Mode.extractOtherBits(permission).imply(bits)) {
      return;
    }

    throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED
        .getMessage(toExceptionMessage(user, bits, path, inode)));
  }

  /**
   * Gets the permission to access an inode path given a user and its groups.
   *
   * @param user the user
   * @param groups the groups this user belongs to
   * @param path the inode path
   * @param inodeList the list of inodes in the path
   * @return the permission
   */
  private Mode.Bits getPermissionInternal(String user, List<String> groups, String path,
      List<Inode<?>> inodeList) {
    int size = inodeList.size();
    Preconditions
        .checkArgument(size > 0, PreconditionMessage.EMPTY_FILE_INFO_LIST_FOR_PERMISSION_CHECK);

    // bypass checking permission for super user or super group of Alluxio file system.
    if (isPrivilegedUser(user, groups)) {
      return Mode.Bits.ALL;
    }

    // traverses from root to the parent dir to all inodes included by this path are executable
    for (int i = 0; i < size - 1; i++) {
      try {
        checkInode(user, groups, inodeList.get(i), Mode.Bits.EXECUTE, path);
      } catch (AccessControlException e) {
        return Mode.Bits.NONE;
      }
    }

    Inode inode = inodeList.get(inodeList.size() - 1);
    if (inode == null) {
      return Mode.Bits.NONE;
    }

    Mode.Bits mode = Mode.Bits.NONE;
    short permission = inode.getMode();
    if (user.equals(inode.getOwner())) {
      mode = mode.or(Mode.extractOwnerBits(permission));
    }
    if (groups.contains(inode.getGroup())) {
      mode = mode.or(Mode.extractGroupBits(permission));
    }
    mode = mode.or(Mode.extractOtherBits(permission));
    return mode;
  }

  private boolean isPrivilegedUser(String user, List<String> groups) {
    return user.equals(mInodeTree.getRootUserName()) || groups.contains(mFileSystemSuperGroup);
  }

  private static String toExceptionMessage(String user, Mode.Bits bits, String path,
      Inode<?> inode) {
    StringBuilder stringBuilder =
        new StringBuilder().append("user=").append(user).append(", ").append("access=").append(bits)
            .append(", ").append("path=").append(path).append(": ").append("failed at ")
            .append(inode.getName().equals("") ? "/" : inode.getName()).append(", inode owner=")
            .append(inode.getOwner()).append(", inode group=").append(inode.getGroup())
            .append(", inode mode=").append(new Mode(inode.getMode()).toString());
    return stringBuilder.toString();
  }
}
