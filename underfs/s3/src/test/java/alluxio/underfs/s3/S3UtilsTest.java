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
import org.jets3t.service.acl.CanonicalGrantee;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.model.StorageOwner;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link S3Utils} methods.
 */
public final class S3UtilsTest {

  /**
   * Tests for {@link S3Utils#translateBucketAcl(AccessControlList, String)}.
   */
  @Test
  public void translateBucketAclTest() {
    // Set up owner id, name and ownerGrantee.
    final String id = "123456789012";
    final String name = "foo";
    final String other = "987654321098";
    StorageOwner owner = new StorageOwner(id /* id */, name /* display name */);
    CanonicalGrantee ownerGrantee = new CanonicalGrantee(id);
    ownerGrantee.setDisplayName(name);

    // Create a acl with the owner.
    AccessControlList acl = new AccessControlList();
    acl.setOwner(owner);

    // Grant only READ, READ_ACP permission to the owner. Check the translated mode is 0500.
    acl.grantPermission(ownerGrantee, Permission.PERMISSION_READ);
    acl.grantPermission(ownerGrantee, Permission.PERMISSION_READ_ACP);
    Assert.assertEquals((short) 0500, S3Utils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0000, S3Utils.translateBucketAcl(acl, other));

    // Grant WRITE permission to the owner. Check the translated mode is 0700.
    acl.grantPermission(ownerGrantee, Permission.PERMISSION_WRITE);
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(acl, id));
    // Add WRITE permission to the owner. Check the translated mode is still 0700.
    acl.grantPermission(ownerGrantee, Permission.PERMISSION_WRITE_ACP);
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0000, S3Utils.translateBucketAcl(acl, other));

    GroupGrantee allUsersGrantee = GroupGrantee.ALL_USERS;
    // Keep the ownerGrantee and permission, while adding another grantee "everyone".
    // Assign READ only permission to "everyone".
    acl.grantPermission(allUsersGrantee, Permission.PERMISSION_READ);
    acl.grantPermission(allUsersGrantee, Permission.PERMISSION_READ_ACP);
    // Check the translated mode is kept 0700.
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0500, S3Utils.translateBucketAcl(acl, other));

    // Revoke all permission for ownerGrantee and only leave permission for "everyone".
    acl.revokeAllPermissions(ownerGrantee);
    // Check the translated mode is now 0500, because owner write permission is revoked.
    Assert.assertEquals((short) 0500, S3Utils.translateBucketAcl(acl, id));
    // Add WRITE permission to "everyone", and check the translated mode becomes 0700.
    acl.grantPermission(allUsersGrantee, Permission.PERMISSION_WRITE);
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(acl, other));

    // Revoke permission to "everyone".
    acl.revokeAllPermissions(allUsersGrantee);
    // Add READ only permission to "all authenticated users".
    GroupGrantee authenticatedUsersGrantee = GroupGrantee.AUTHENTICATED_USERS;
    acl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_READ);
    acl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_READ_ACP);
    // Check the mode is 0500.
    Assert.assertEquals((short) 0500, S3Utils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0500, S3Utils.translateBucketAcl(acl, other));
    // Add WRITE permission to "all authenticated users" and check permission.
    acl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_WRITE);
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(acl, other));
  }
}
