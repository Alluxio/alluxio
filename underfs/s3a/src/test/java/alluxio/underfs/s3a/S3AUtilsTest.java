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
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link S3AUtils} methods.
 */
public final class S3AUtilsTest {

  /**
   * Tests for {@link S3AUtils#translateBucketAcl(AccessControlList, String)}.
   */
  @Test
  public void translateBucketAclTest() {
    // Set up owner id, name and ownerGrantee.
    final String id = "123456789012";
    final String name = "foo";
    final String other = "987654321098";
    Owner owner = new Owner(id /* id */, name /* display name */);
    CanonicalGrantee ownerGrantee = new CanonicalGrantee(id);
    ownerGrantee.setDisplayName(name);

    // Create a acl with the owner.
    AccessControlList acl = new AccessControlList();
    acl.setOwner(owner);

    // Grant only READ, READ_ACP permission to the owner. Check the translated mode is 0500.
    acl.grantPermission(ownerGrantee, Permission.Read);
    acl.grantPermission(ownerGrantee, Permission.ReadAcp);
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0000, S3AUtils.translateBucketAcl(acl, other));

    // Grant WRITE permission to the owner. Check the translated mode is 0700.
    acl.grantPermission(ownerGrantee, Permission.Write);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(acl, id));
    // Add WRITE permission to the owner. Check the translated mode is still 0700.
    acl.grantPermission(ownerGrantee, Permission.WriteAcp);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0000, S3AUtils.translateBucketAcl(acl, other));

    GroupGrantee allUsersGrantee = GroupGrantee.AllUsers;
    // Keep the ownerGrantee and permission, while adding another grantee "everyone".
    // Assign READ only permission to "everyone".
    acl.grantPermission(allUsersGrantee, Permission.Read);
    acl.grantPermission(allUsersGrantee, Permission.ReadAcp);
    // Check the translated mode is kept 0700.
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(acl, other));

    // Revoke all permission for ownerGrantee and only leave permission for "everyone".
    acl.revokeAllPermissions(ownerGrantee);
    // Check the translated mode is now 0500, because owner write permission is revoked.
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(acl, id));
    // Add WRITE permission to "everyone", and check the translated mode becomes 0700.
    acl.grantPermission(allUsersGrantee, Permission.Write);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(acl, other));

    // Revoke permission to "everyone".
    acl.revokeAllPermissions(allUsersGrantee);
    // Add READ only permission to "all authenticated users".
    GroupGrantee authenticatedUsersGrantee = GroupGrantee.AuthenticatedUsers;
    acl.grantPermission(authenticatedUsersGrantee, Permission.Read);
    acl.grantPermission(authenticatedUsersGrantee, Permission.ReadAcp);
    // Check the mode is 0500.
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(acl, other));
    // Add WRITE permission to "all authenticated users" and check permission.
    acl.grantPermission(authenticatedUsersGrantee, Permission.Write);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(acl, id));
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(acl, other));
  }
}
