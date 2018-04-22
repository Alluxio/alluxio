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

import alluxio.master.file.meta.Inode;
import alluxio.security.authorization.Mode;

import java.util.List;

/**
 * Checks and gets permitted actions on Inodes.
 */
public interface InodePermissionChecker {
  /**
   * @return a new instance of {@link InodePermissionChecker}
   */
  static InodePermissionChecker create() {
    return new DefaultInodePermissionChecker();
  }

  /**
   * Checks whether user has the specified permission on the file or directory at path.
   *
   * @param user the user
   * @param groups the groups that user belongs to
   * @param inode inode of the path
   * @param permission the permissions to check
   * @return whether the user has the required permission
   */
  boolean checkPermission(String user, List<String> groups, Inode<?> inode, Mode.Bits permission);

  /**
   * Gets the permission the user has for the file or directory at path.
   *
   * @param user the user
   * @param groups the groups that user belongs to
   * @param inode inode of the path
   * @return the permission
   */
  Mode.Bits getPermission(String user, List<String> groups, Inode<?> inode);
}
