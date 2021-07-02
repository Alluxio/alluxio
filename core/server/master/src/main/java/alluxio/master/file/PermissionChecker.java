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

import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.security.authorization.Mode;

/**
 * interface to provide permission check logic.
 */
public interface PermissionChecker {
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
  void checkParentPermission(Mode.Bits bits, LockedInodePath inodePath)
      throws AccessControlException, InvalidPathException;

  /**
   * Checks whether a user has permission to perform a specific action on a path. This check will
   * pass if the path is invalid.
   *
   * @param bits bits that capture the action {@link Mode.Bits} by user
   * @param inodePath the path to check permission on
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  void checkPermission(Mode.Bits bits, LockedInodePath inodePath)
      throws AccessControlException;

  /**
   * Checks whether the user is a super user or in super group.
   *
   * @throws AccessControlException if the user is not a super user
   */
  void checkSuperUser() throws AccessControlException;

  /**
   * Gets the permission to access inodePath for the current client user.
   *
   * @param inodePath the inode path
   * @return the permission
   */
  Mode.Bits getPermission(LockedInodePath inodePath);

  /**
   * Checks whether a user has permission to edit the attribute of a given path.
   *
   * @param inodePath the path to check permission on
   * @param superuserRequired indicates whether it requires to be the superuser
   * @param ownerRequired indicates whether it requires to be the owner of this path
   * @param writeRequired indicates whether it requires to have write permissions
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  void checkSetAttributePermission(LockedInodePath inodePath, boolean superuserRequired,
      boolean ownerRequired, boolean writeRequired)
      throws AccessControlException, InvalidPathException;
}
