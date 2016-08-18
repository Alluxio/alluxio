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

import org.jets3t.service.acl.CanonicalGrantee;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.model.StorageOwner;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link GCSUtils} methods.
 */
public final class GCSUtilsTest {

  private static final String NAME = "foo";
  private static final String ID = "123456789012";
  private static final String OTHER = "987654321098";

  private CanonicalGrantee mOwnerGrantee;
  private GSAccessControlList mAcl;

  @Before
  public void before() throws Exception {
    // Setup owner.
    mOwnerGrantee = new CanonicalGrantee(ID);
    mOwnerGrantee.setDisplayName(NAME);

    // Setup the acl.
    mAcl = new GSAccessControlList();
    mAcl.setOwner(new StorageOwner(ID, NAME));
  }

  @Test
  public void translateUserAcl() {
    // Grant only READ, READ_ACP permission to the user. Check the translated mode is 0500.
    mAcl.grantPermission(mOwnerGrantee, Permission.PERMISSION_READ);
    mAcl.grantPermission(mOwnerGrantee, Permission.PERMISSION_READ_ACP);
    Assert.assertEquals((short) 0500, GCSUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0000, GCSUtils.translateBucketAcl(mAcl, OTHER));

    // Grant WRITE permission to the user. Check the translated mode is 0700.
    mAcl.grantPermission(mOwnerGrantee, Permission.PERMISSION_WRITE);
    Assert.assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, ID));
    // Add WRITE_ACP permission to the user. Check the translated mode is still 0700.
    mAcl.grantPermission(mOwnerGrantee, Permission.PERMISSION_WRITE_ACP);
    Assert.assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0000, GCSUtils.translateBucketAcl(mAcl, OTHER));
  }

  @Test
  public void translateUserFullPermission() {
    mAcl.grantPermission(mOwnerGrantee, Permission.PERMISSION_FULL_CONTROL);
    Assert.assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0000, GCSUtils.translateBucketAcl(mAcl, OTHER));
  }

  @Test
  public void translateEveryoneAcl() {
    GroupGrantee allUsersGrantee = GroupGrantee.ALL_USERS;
    // Assign READ only permission to "everyone".
    mAcl.grantPermission(allUsersGrantee, Permission.PERMISSION_READ);
    mAcl.grantPermission(allUsersGrantee, Permission.PERMISSION_READ_ACP);
    // Check the translated mode is now 0500.
    Assert.assertEquals((short) 0500, GCSUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0500, GCSUtils.translateBucketAcl(mAcl, OTHER));
    // Add WRITE permission to "everyone", and check the translated mode becomes 0700.
    mAcl.grantPermission(allUsersGrantee, Permission.PERMISSION_WRITE);
    Assert.assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, OTHER));
  }

  @Test
  public void translateAuthenticatedUserAcl() {
    // Add READ only permission to "all authenticated users".
    GroupGrantee authenticatedUsersGrantee = GroupGrantee.AUTHENTICATED_USERS;
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_READ);
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_READ_ACP);
    // Check the mode is 0500.
    Assert.assertEquals((short) 0500, GCSUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0500, GCSUtils.translateBucketAcl(mAcl, OTHER));
    // Add WRITE permission to "all authenticated users" and check permission.
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_WRITE);
    Assert.assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, OTHER));
  }
}
