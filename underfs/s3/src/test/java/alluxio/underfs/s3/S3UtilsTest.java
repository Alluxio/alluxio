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
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link S3Utils} methods.
 */
public final class S3UtilsTest {

  private static final String NAME = "foo";
  private static final String ID = "123456789012";
  private static final String OTHER = "987654321098";

  private CanonicalGrantee mOwnerGrantee;
  private AccessControlList mAcl;

  @Before
  public void before() throws Exception {
    // Set up owner id, name and mOwnerGrantee.
    StorageOwner owner = new StorageOwner(ID /* id */, NAME /* display name */);
    mOwnerGrantee = new CanonicalGrantee(ID);
    mOwnerGrantee.setDisplayName(NAME);

    // Create a mAcl with the owner.
    mAcl = new AccessControlList();
    mAcl.setOwner(owner);
  }

  /**
   * Tests for {@link S3Utils#translateBucketAcl(AccessControlList, String)}.
   */
  @Test
  public void translateOwnerAclTest() {
    // Grant only READ, READ_ACP permission to the owner. Check the translated mode is 0500.
    mAcl.grantPermission(mOwnerGrantee, Permission.PERMISSION_READ);
    mAcl.grantPermission(mOwnerGrantee, Permission.PERMISSION_READ_ACP);
    Assert.assertEquals((short) 0500, S3Utils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0000, S3Utils.translateBucketAcl(mAcl, OTHER));

    // Grant WRITE permission to the owner. Check the translated mode is 0700.
    mAcl.grantPermission(mOwnerGrantee, Permission.PERMISSION_WRITE);
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(mAcl, ID));
    // Add WRITE_ACP permission to the owner. Check the translated mode is still 0700.
    mAcl.grantPermission(mOwnerGrantee, Permission.PERMISSION_WRITE_ACP);
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0000, S3Utils.translateBucketAcl(mAcl, OTHER));

    // Revoke permission to owner.
    mAcl.revokeAllPermissions(mOwnerGrantee);
  }

  /**
   * Tests for translating bucket acl granted to "everyone".
   */
  @Test
  public void translateEveryoneAclTest() {
    GroupGrantee allUsersGrantee = GroupGrantee.ALL_USERS;
    // Assign READ only permission to "everyone".
    mAcl.grantPermission(allUsersGrantee, Permission.PERMISSION_READ);
    mAcl.grantPermission(allUsersGrantee, Permission.PERMISSION_READ_ACP);
    // Check the translated mode is now 0500, because owner write permission is revoked.
    Assert.assertEquals((short) 0500, S3Utils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0500, S3Utils.translateBucketAcl(mAcl, OTHER));
    // Add WRITE permission to "everyone", and check the translated mode becomes 0700.
    mAcl.grantPermission(allUsersGrantee, Permission.PERMISSION_WRITE);
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(mAcl, OTHER));

    // Revoke permission to "everyone".
    mAcl.revokeAllPermissions(allUsersGrantee);
  }

  /**
   * Tests for translating bucket acl granted to all authenticated users.
   */
  @Test
  public void translateAuthenticatedUserAclTest() {
    // Add READ only permission to "all authenticated users".
    GroupGrantee authenticatedUsersGrantee = GroupGrantee.AUTHENTICATED_USERS;
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_READ);
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_READ_ACP);
    // Check the mode is 0500.
    Assert.assertEquals((short) 0500, S3Utils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0500, S3Utils.translateBucketAcl(mAcl, OTHER));
    // Add WRITE permission to "all authenticated users" and check permission.
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_WRITE);
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0700, S3Utils.translateBucketAcl(mAcl, OTHER));
    mAcl.revokeAllPermissions(authenticatedUsersGrantee);
  }
}
