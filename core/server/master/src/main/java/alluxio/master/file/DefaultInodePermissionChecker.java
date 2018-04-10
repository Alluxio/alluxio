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
import alluxio.exception.ExceptionMessage;
import alluxio.master.file.meta.Inode;
import alluxio.security.authorization.Mode;

import java.util.List;

/**
 * {@link InodePermissionChecker} implementation using standard POSIX permission model.
 */
public final class DefaultInodePermissionChecker implements InodePermissionChecker {
  @Override
  public void checkPermission(String user, List<String> groups, String path, Inode<?> inode,
      Mode.Bits permission) throws AccessControlException {
    short mode = inode.getMode();
    if (user.equals(inode.getOwner()) && Mode.extractOwnerBits(mode).imply(permission)) {
      return;
    }
    if (groups.contains(inode.getGroup()) && Mode.extractGroupBits(mode).imply(permission)) {
      return;
    }
    if (Mode.extractOtherBits(mode).imply(permission)) {
      return;
    }
    throw new AccessControlException(ExceptionMessage.PERMISSION_DENIED
        .getMessage(toExceptionMessage(user, permission, path, inode)));
  }

  @Override
  public Mode.Bits getPermission(String user, List<String> groups, String path, Inode<?> inode) {
    Mode.Bits permission = Mode.Bits.NONE;
    short mode = inode.getMode();
    if (user.equals(inode.getOwner())) {
      permission = permission.or(Mode.extractOwnerBits(mode));
    }
    if (groups.contains(inode.getGroup())) {
      permission = permission.or(Mode.extractGroupBits(mode));
    }
    permission = permission.or(Mode.extractOtherBits(mode));
    return permission;
  }

  private static String toExceptionMessage(String user, Mode.Bits bits, String path,
      Inode<?> inode) {
    StringBuilder sb =
        new StringBuilder().append("user=").append(user).append(", ").append("access=").append(bits)
            .append(", ").append("path=").append(path).append(": ").append("failed at ")
            .append(inode.getName().equals("") ? "/" : inode.getName()).append(", inode owner=")
            .append(inode.getOwner()).append(", inode group=").append(inode.getGroup())
            .append(", inode mode=").append(new Mode(inode.getMode()).toString());
    return sb.toString();
  }
}
