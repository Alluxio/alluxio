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

package alluxio.underfs.gcs;

import org.jets3t.service.acl.GrantAndPermission;
import org.jets3t.service.acl.GranteeInterface;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.acl.gs.GSAccessControlList;

/**
 * Util functions for GCS under file system.
 */
public final class GCSUtils {
  /**
   * Translates GCS bucket owner ACL to Alluxio owner mode.
   *
   * @param acl the acl of GCS bucket
   * @param userId the S3 user id of the Alluxio owner
   * @return the translated posix mode in short format
   */
  public static short translateBucketAcl(GSAccessControlList acl, String userId) {
    short mode = (short) 0;
    for (GrantAndPermission gp : acl.getGrantAndPermissions()) {
      Permission perm = gp.getPermission();
      GranteeInterface grantee = gp.getGrantee();
      if (perm.equals(Permission.PERMISSION_READ)) {
        if (isUserIdInGrantee(grantee, userId)) {
          // If the bucket is readable by the user, add r and x to the owner mode.
          mode |= (short) 0500;
        }
      } else if (perm.equals(Permission.PERMISSION_WRITE)) {
        if (isUserIdInGrantee(grantee, userId)) {
          // If the bucket is writable by the user, +w to the owner mode.
          mode |= (short) 0200;
        }
      } else if (perm.equals(Permission.PERMISSION_FULL_CONTROL)) {
        if (isUserIdInGrantee(grantee, userId)) {
          // If the user has full control to the bucket, +rwx to the owner mode.
          mode |= (short) 0700;
        }
      }
    }
    return mode;
  }

  private static boolean isUserIdInGrantee(GranteeInterface grantee, String userId) {
    return grantee.getIdentifier().equals(userId)
        || grantee.equals(GroupGrantee.ALL_USERS)
        || grantee.equals(GroupGrantee.AUTHENTICATED_USERS);
  }

  private GCSUtils() {} // prevent instantiation
}

