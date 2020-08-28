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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Default implementation of permission checker.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public class DefaultPermissionChecker implements PermissionChecker {
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
  public DefaultPermissionChecker(InodeTree inodeTree) {
    mInodeTree = Preconditions.checkNotNull(inodeTree, "inodeTree");
    mPermissionCheckEnabled =
        ServerConfiguration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED);
    mFileSystemSuperGroup =
        ServerConfiguration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP);
  }

  @Override
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
    List<InodeView> inodeList = inodePath.getInodeViewList();

    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser(ServerConfiguration.global());
    List<String> groups = getGroups(user);

    // remove the last element if all components of the path exist, since we only check the parent.
    if (inodePath.fullPathExists()) {
      inodeList.remove(inodeList.size() - 1);
    }
    checkInodeList(user, groups, bits, inodePath.getUri().getPath(), inodeList, false);
  }

  @Override
  public void checkPermission(Mode.Bits bits, LockedInodePath inodePath)
      throws AccessControlException {
    if (!mPermissionCheckEnabled) {
      return;
    }

    // collects inodes info on the path
    List<InodeView> inodeList = inodePath.getInodeViewList();

    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser(ServerConfiguration.global());
    List<String> groups = getGroups(user);

    checkInodeList(user, groups, bits, inodePath.getUri().getPath(), inodeList, false);
  }

  @Override
  public Mode.Bits getPermission(LockedInodePath inodePath) {
    if (!mPermissionCheckEnabled) {
      return Mode.Bits.NONE;
    }
    // collects inodes info on the path
    List<InodeView> inodeList = inodePath.getInodeViewList();

    // collects user and groups
    try {
      String user = AuthenticatedClientUser.getClientUser(ServerConfiguration.global());
      List<String> groups = getGroups(user);
      return getPermissionInternal(user, groups, inodePath.getUri().getPath(), inodeList);
    } catch (AccessControlException e) {
      return Mode.Bits.NONE;
    }
  }

  @Override
  public void checkSetAttributePermission(LockedInodePath inodePath, boolean superuserRequired,
      boolean ownerRequired, boolean writeRequired)
      throws AccessControlException, InvalidPathException {
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
    // For other non-permission related attributes
    if (writeRequired) {
      checkPermission(Mode.Bits.WRITE, inodePath);
    }
  }

  @Override
  public void checkSuperUser() throws AccessControlException {
    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser(ServerConfiguration.global());
    List<String> groups = getGroups(user);
    if (!isPrivilegedUser(user, groups)) {
      throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED
          .getMessage(user + " is not a super user or in super group"));
    }
  }

  /**
   * @param user the user to get groups for
   * @return the groups for the given user
   * @throws AccessControlException if the group service information cannot be accessed
   */
  private List<String> getGroups(String user) throws AccessControlException {
    try {
      return CommonUtils.getGroups(user, ServerConfiguration.global());
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
    List<InodeView> inodeList = inodePath.getInodeViewList();

    // collects user and groups
    String user = AuthenticatedClientUser.getClientUser(ServerConfiguration.global());
    List<String> groups = getGroups(user);

    if (isPrivilegedUser(user, groups)) {
      return;
    }

    checkInodeList(user, groups, null, inodePath.getUri().getPath(), inodeList, true);
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
  protected void checkInodeList(String user, List<String> groups, Mode.Bits bits, String path,
      List<InodeView> inodeList, boolean checkIsOwner) throws AccessControlException {
    int size = inodeList.size();
    Preconditions.checkArgument(size > 0,
        PreconditionMessage.EMPTY_FILE_INFO_LIST_FOR_PERMISSION_CHECK);

    // bypass checking permission for super user or super group of Alluxio file system.
    if (isPrivilegedUser(user, groups)) {
      return;
    }

    // traverses from root to the parent dir to all inodes included by this path are executable
    for (int i = 0; i < size - 1; i++) {
      checkInode(user, groups, inodeList.get(i), Mode.Bits.EXECUTE, path);
    }

    InodeView inode = inodeList.get(inodeList.size() - 1);
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
  private void checkInode(String user, List<String> groups, InodeView inode, Mode.Bits bits,
      String path) throws AccessControlException {
    if (inode == null) {
      return;
    }
    for (AclAction action : bits.toAclActionSet()) {
      if (!inode.checkPermission(user, groups, action)) {
        throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED
            .getMessage(toExceptionMessage(user, bits, path, inode)));
      }
    }
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
      List<InodeView> inodeList) {
    int size = inodeList.size();
    Preconditions.checkArgument(size > 0,
        PreconditionMessage.EMPTY_FILE_INFO_LIST_FOR_PERMISSION_CHECK);

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

    InodeView inode = inodeList.get(inodeList.size() - 1);
    if (inode == null) {
      return Mode.Bits.NONE;
    }

    return inode.getPermission(user, groups).toModeBits();
  }

  private boolean isPrivilegedUser(String user, List<String> groups) {
    return user.equals(mInodeTree.getRootUserName()) || groups.contains(mFileSystemSuperGroup);
  }

  private static String toExceptionMessage(String user, Mode.Bits bits, String path,
      InodeView inode) {
    StringBuilder sb =
        new StringBuilder().append("user=").append(user).append(", ").append("access=").append(bits)
            .append(", ").append("path=").append(path).append(": ").append("failed at ")
            .append(inode.getName().equals("") ? "/" : inode.getName()).append(", inode owner=")
            .append(inode.getOwner()).append(", inode group=").append(inode.getGroup())
            .append(", inode mode=").append(new Mode(inode.getMode()).toString());
    return sb.toString();
  }
}
