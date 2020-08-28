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

package alluxio.underfs.s3a;

import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;

/**
 * Util functions for S3A under file system.
 */
public final class S3AUtils {
  /**
   * Translates S3 bucket ACL to Alluxio owner mode.
   *
   * @param acl the acl of S3 bucket
   * @param userId the S3 user id of the Alluxio owner
   * @return the translated posix mode in short format
   */
  public static short translateBucketAcl(AccessControlList acl, String userId) {
    short mode = (short) 0;
    for (Grant grant : acl.getGrantsAsList()) {
      Permission perm = grant.getPermission();
      Grantee grantee = grant.getGrantee();
      if (perm.equals(Permission.Read)) {
        if (isUserIdInGrantee(grantee, userId)) {
          // If the bucket is readable by the user, add r and x to the owner mode.
          mode |= (short) 0500;
        }
      } else if (perm.equals(Permission.Write)) {
        if (isUserIdInGrantee(grantee, userId)) {
          // If the bucket is writable by the user, +w to the owner mode.
          mode |= (short) 0200;
        }
      } else if (perm.equals(Permission.FullControl)) {
        if (isUserIdInGrantee(grantee, userId)) {
          // If the user has full control to the bucket, +rwx to the owner mode.
          mode |= (short) 0700;
        }
      }
    }
    return mode;
  }

  private static boolean isUserIdInGrantee(Grantee grantee, String userId) {
    return grantee.getIdentifier() != null && grantee.getIdentifier().equals(userId)
        || grantee.equals(GroupGrantee.AllUsers)
        || grantee.equals(GroupGrantee.AuthenticatedUsers);
  }

  private S3AUtils() {} // prevent instantiation
}
