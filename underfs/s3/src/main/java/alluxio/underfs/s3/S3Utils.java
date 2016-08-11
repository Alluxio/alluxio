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

package alluxio.underfs.s3;

import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.acl.GrantAndPermission;
import org.jets3t.service.acl.GranteeInterface;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;

/**
 * Util functions for S3N under file system.
 */
public final class S3Utils {
  /**
   * Translates S3 bucket owner ACL to Alluxio owner mode.
   *
   * @param acl the acl of S3 bucket
   * @param bucketOwnerId the bucket owner id
   * @return the translated posix mode in short format
   */
  public static short translateBucketAcl(AccessControlList acl, String bucketOwnerId) {
    short mode = (short) 0;
    for (GrantAndPermission gp : acl.getGrantAndPermissions()) {
      Permission perm = gp.getPermission();
      GranteeInterface grantee = gp.getGrantee();
      // If the bucket is readable by the owner, add r and x to the owner mode.
      if (perm.equals(Permission.PERMISSION_READ)
          && (grantee.getIdentifier().equals(bucketOwnerId)
              || grantee.equals(GroupGrantee.ALL_USERS)
              || grantee.equals(GroupGrantee.AUTHENTICATED_USERS))) {
        mode |= (short) 0500;
      }
      // If the bucket is writable by the owner, +w to the owner mode.
      if (perm.equals(Permission.PERMISSION_WRITE)
          && (grantee.getIdentifier().equals(bucketOwnerId)
              || grantee.equals(GroupGrantee.ALL_USERS)
              || grantee.equals(GroupGrantee.AUTHENTICATED_USERS))) {
        mode |= (short) 0200;
      }
    }
    return mode;
  }

  private S3Utils() {} // prevent instantiation
}
