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
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link S3AUtils} methods.
 */
public final class S3AUtilsTest {
  private static final String NAME = "foo";
  private static final String ID = "123456789012";
  private static final String OTHER = "987654321098";

  private CanonicalGrantee mOwnerGrantee;
  private AccessControlList mAcl;

  @Before
  public void before() throws Exception {
    // Set up owner id, name and mOwnerGrantee.
    Owner owner = new Owner(ID /* id */, NAME /* display name */);
    mOwnerGrantee = new CanonicalGrantee(ID);
    mOwnerGrantee.setDisplayName(NAME);

    // Create a mAcl with the owner.
    mAcl = new AccessControlList();
    mAcl.setOwner(owner);
  }

  @Test
  public void translateOwnerAclTest() {
    // Grant only READ, READ_ACP permission to the owner. Check the translated mode is 0500.
    mAcl.grantPermission(mOwnerGrantee, Permission.Read);
    mAcl.grantPermission(mOwnerGrantee, Permission.ReadAcp);
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0000, S3AUtils.translateBucketAcl(mAcl, OTHER));

    // Grant WRITE permission to the owner. Check the translated mode is 0700.
    mAcl.grantPermission(mOwnerGrantee, Permission.Write);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, ID));
    // Add WRITE_ACP permission to the owner. Check the translated mode is still 0700.
    mAcl.grantPermission(mOwnerGrantee, Permission.WriteAcp);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0000, S3AUtils.translateBucketAcl(mAcl, OTHER));
  }

  @Test
  public void translateEveryoneAclTest() {
    GroupGrantee allUsersGrantee = GroupGrantee.AllUsers;
    // Assign READ only permission to "everyone".
    mAcl.grantPermission(allUsersGrantee, Permission.Read);
    mAcl.grantPermission(allUsersGrantee, Permission.ReadAcp);
    // Check the translated mode is now 0500, because owner write permission is revoked.
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(mAcl, OTHER));
    // Add WRITE permission to "everyone", and check the translated mode becomes 0700.
    mAcl.grantPermission(allUsersGrantee, Permission.Write);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, OTHER));
  }

  @Test
  public void translateAuthenticatedUserAclTest() {
    // Add READ only permission to "all authenticated users".
    GroupGrantee authenticatedUsersGrantee = GroupGrantee.AuthenticatedUsers;
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.Read);
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.ReadAcp);
    // Check the mode is 0500.
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(mAcl, OTHER));
    // Add WRITE permission to "all authenticated users" and check permission.
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.Write);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, OTHER));
  }
}
