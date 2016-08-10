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
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;

/**
 * Util functions for S3A under file system.
 */
public final class S3AUtils {
  /**
   * Translates S3 bucket owner ACL to Alluxio owner mode.
   *
   * @param acl the acl of S3 bucket
   * @param bucketOwnerId the bucket owner id
   * @return the translated posix mode in short format
   */
  public static short translateBucketAcl(AccessControlList acl, String bucketOwnerId) {
    short mode = (short) 0;
    for (Grant grant : acl.getGrantsAsList()) {
      // If the bucket is readable by the owner, add r and x to the owner mode.
      if (grant.getPermission().equals(Permission.Read)) {
        if (grant.getGrantee().getIdentifier().equals(bucketOwnerId)
            || grant.getGrantee().equals(GroupGrantee.AllUsers)
            || grant.getGrantee().equals(GroupGrantee.AuthenticatedUsers)) {
          mode |= (short) 0500;
        }
      }
    }
    for (Grant grant : acl.getGrantsAsList()) {
      // If the bucket is writable by the owner, +w to the owner mode.
      if (grant.getPermission().equals(Permission.Write)) {
        if (grant.getGrantee().getIdentifier().equals(bucketOwnerId)
            || grant.getGrantee().equals(GroupGrantee.AllUsers)
            || grant.getGrantee().equals(GroupGrantee.AuthenticatedUsers)) {
          mode |= (short) 0200;
        }
      }
    }
    return mode;
  }

  private S3AUtils() {} // prevent instantiation
}
