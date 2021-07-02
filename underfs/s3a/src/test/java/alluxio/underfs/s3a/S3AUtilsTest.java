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
  private static final String OTHER_ID = "987654321098";

  private CanonicalGrantee mUserGrantee;
  private AccessControlList mAcl;

  @Before
  public void before() throws Exception {
    // Setup owner.
    mUserGrantee = new CanonicalGrantee(ID);
    mUserGrantee.setDisplayName(NAME);

    // Setup the acl.
    mAcl = new AccessControlList();
    mAcl.setOwner(new Owner(ID, NAME));
  }

  @Test
  public void translateUserReadPermission() {
    mAcl.grantPermission(mUserGrantee, Permission.Read);
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0000, S3AUtils.translateBucketAcl(mAcl, OTHER_ID));
    mAcl.grantPermission(mUserGrantee, Permission.ReadAcp);
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0000, S3AUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateUserWritePermission() {
    mAcl.grantPermission(mUserGrantee, Permission.Write);
    Assert.assertEquals((short) 0200, S3AUtils.translateBucketAcl(mAcl, ID));
    mAcl.grantPermission(mUserGrantee, Permission.Read);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, ID));
  }

  @Test
  public void translateUserFullPermission() {
    mAcl.grantPermission(mUserGrantee, Permission.FullControl);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0000, S3AUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateEveryoneReadPermission() {
    GroupGrantee allUsersGrantee = GroupGrantee.AllUsers;
    mAcl.grantPermission(allUsersGrantee, Permission.Read);
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateEveryoneWritePermission() {
    GroupGrantee allUsersGrantee = GroupGrantee.AllUsers;
    mAcl.grantPermission(allUsersGrantee, Permission.Write);
    Assert.assertEquals((short) 0200, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0200, S3AUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateEveryoneFullPermission() {
    GroupGrantee allUsersGrantee = GroupGrantee.AllUsers;
    mAcl.grantPermission(allUsersGrantee, Permission.FullControl);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateAuthenticatedUserReadPermission() {
    GroupGrantee authenticatedUsersGrantee = GroupGrantee.AuthenticatedUsers;
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.Read);
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0500, S3AUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateAuthenticatedUserWritePermission() {
    GroupGrantee authenticatedUsersGrantee = GroupGrantee.AuthenticatedUsers;
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.Write);
    Assert.assertEquals((short) 0200, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0200, S3AUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateAuthenticatedUserFullPermission() {
    GroupGrantee authenticatedUsersGrantee = GroupGrantee.AuthenticatedUsers;
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.FullControl);
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, ID));
    Assert.assertEquals((short) 0700, S3AUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translatePermissionWithNullId() {
    // Emulate a corner case when returned grantee does not have ID from some S3 compatible UFS
    mUserGrantee.setIdentifier(null);
    mAcl.grantPermission(mUserGrantee, Permission.Read);
    Assert.assertEquals((short) 0000, S3AUtils.translateBucketAcl(mAcl, OTHER_ID));
  }
}
