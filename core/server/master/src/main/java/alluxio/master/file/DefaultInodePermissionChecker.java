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
 * {@link InodePermissionChecker} implementation using standard POSIX permission model.
 */
public final class DefaultInodePermissionChecker implements InodePermissionChecker {
  @Override
  public boolean checkPermission(String user, List<String> groups, Inode<?> inode,
      Mode.Bits permission) {
    short mode = inode.getMode();
    if (user.equals(inode.getOwner())) {
      return Mode.extractOwnerBits(mode).imply(permission);
    }
    if (groups.contains(inode.getGroup())) {
      return Mode.extractGroupBits(mode).imply(permission);
    }
    return Mode.extractOtherBits(mode).imply(permission);
  }

  @Override
  public Mode.Bits getPermission(String user, List<String> groups, Inode<?> inode) {
    short mode = inode.getMode();
    if (user.equals(inode.getOwner())) {
      return Mode.extractOwnerBits(mode);
    }
    if (groups.contains(inode.getGroup())) {
      return Mode.extractGroupBits(mode);
    }
    return Mode.extractOtherBits(mode);
  }
}
