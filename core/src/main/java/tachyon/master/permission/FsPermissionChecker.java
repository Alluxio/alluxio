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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.master.Inode;
import tachyon.master.InodesInPath;
import tachyon.master.permission.AclEntry.AclPermission;
import tachyon.security.UserGroup;
import tachyon.thrift.AccessControlException;

/**
 * Class that helps in checking file system permission.
 * The state of this class need not be synchronized as it has data structures that
 * are read-only.
 */
public class FsPermissionChecker implements AccessControlEnforcer {
  static final Logger LOG = LoggerFactory.getLogger(FsPermissionChecker.class);
  private final String mFsOwner;
  private final String mSupergroup;
  private final String mUser;
  private final List<String> mGroups;
  private final boolean mIsSuperUser;
  private final UserGroup mCaller;

  public FsPermissionChecker(String fsOwner, String supergroup,
      UserGroup caller) {
    this.mFsOwner = fsOwner;
    this.mSupergroup = supergroup;
    this.mCaller = caller;
    this.mUser = caller.getShortUserName();
    this.mGroups = caller.getGroupNames();
    this.mIsSuperUser = fsOwner.equals(mUser) || mGroups.contains(supergroup);
  }

  public String getFsOwner() {
    return mFsOwner;
  }

  public String getUser() {
    return mUser;
  }

  public List<String> getGroups() {
    return mGroups;
  }

  /**
   * Check whether current user have permissions to access the path.
   * Traverse is always checked.
   * 
   * Parent path means the parent directory for the path.
   * Ancestor path means the last (the closest) existing ancestor directory
   * of the path.
   * Note that if the parent path exists,
   * then the parent path and the ancestor path are the same.
   * 
   * For example, suppose the path is "/foo/bar/baz".
   * No matter baz is a file or a directory,
   * the parent path is "/foo/bar".
   * If bar exists, then the ancestor path is also "/foo/bar".
   * If bar does not exist and foo exists,
   * then the ancestor path is "/foo".
   * Further, if both foo and bar do not exist,
   * then the ancestor path is "/".
   * 
   * @param Inodes information to been checked
   * @param doCheckOwner Require user to be the owner of the path?
   * @param ancestorAccess The access required by the ancestor of the path.
   * @param parentAccess The access required by the parent of the path.
   * @param access The access required by the path.
   * @param subAccess If path is a directory,
   * @throws AccessControlException
   */
  public void check(InodesInPath iip, boolean doCheckOwner,
      AclPermission ancestorAccess, AclPermission parentAccess, AclPermission access)
      throws AccessControlException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ACCESS CHECK: " + this
          + ", doCheckOwner=" + doCheckOwner
          + ", ancestorAccess=" + ancestorAccess
          + ", parentAccess=" + parentAccess
          + ", access=" + access);
    }
    if (mIsSuperUser) {
      return;
    }

    checkPermission(mFsOwner, mSupergroup, mCaller, iip.getInodes(),
        iip.getPathByNameArr(), iip.getFullPath(), iip.getInodes().length - 2,
        ancestorAccess, parentAccess, access, doCheckOwner);
  }

  private void check(Inode[] inodes, String path, int i, AclPermission access)
      throws AccessControlException {
    check(i >= 0 ? inodes[i] : null, path, access);
  }

  private void check(Inode inode, String path, AclPermission access)
      throws AccessControlException {
    if (inode == null) {
      return;
    }
    final Acl acl = inode.getAcl();
    if (getUser().equals(acl.getUserName())) {
      //user permission
      if (acl.getUserPermission().implies(access)) {
        return;
      }
    } else if (getGroups().contains(acl.getGroupName())) {
      //group permission
      if (acl.getGroupPermission().implies(access)) {
        return;
      }
    } else { //other permission
      if (acl.getOtherPermission().implies(access)) {
        return;
      }
    }
    throw new AccessControlException(
        toAccessControlString(inode, path, access));
  }

  private void checkOwner(Inode inode)
      throws AccessControlException {
    if (getUser().equals(inode.getAcl().getUserName())) {
      return;
    }
    throw new AccessControlException(
            "Permission denied. user="
            + getUser() + " is not the owner of inode=" + inode);
  }

  private void checkTraverse(Inode[] inodes, String path, int last)
      throws AccessControlException {
    for (int j = 0; j <= last; j++) {
      check(inodes[j], path, AclPermission.EXECUTE);
    }
  }

  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(Inode inode, String path, AclPermission access) {
    StringBuilder sb = new StringBuilder("Permission denied: ")
        .append("user=").append(getUser()).append(", ")
        .append("access=").append(access).append(", ")
        .append("inode=\"").append(path).append("\":")
        .append(inode.getAcl().getUserName()).append(':')
        .append(inode.getAcl().getGroupName()).append(':')
        .append(inode.isDirectory() ? 'd' : '-')
        .append(inode.getAcl().getPermission());
    return sb.toString();
  }

  @Override
  public void checkPermission(String fsOwner, String supergroup,
      UserGroup caller, Inode[] inodes, String[] pathByNameArr, String path,
      int ancestorIndex, AclPermission ancestorAccess, AclPermission parentAccess,
      AclPermission access, boolean doCheckOwner) throws AccessControlException {
    for (; ancestorIndex >= 0 && inodes[ancestorIndex] == null; ancestorIndex-- ) {
      checkTraverse(inodes, path, ancestorIndex);
    }

    if (ancestorAccess != null && inodes.length > 1) {
      check(inodes, path, ancestorIndex, ancestorAccess);
    }

    if (parentAccess != null && inodes.length > 1) {
      check(inodes, path, inodes.length - 2, parentAccess);
    }

    if (access != null && inodes.length > 0) {
      check(inodes[inodes.length - 1], path, access);
    }
    if (doCheckOwner && inodes.length > 0) {
      checkOwner(inodes[inodes.length - 1]);
    }
  }
}
