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

import static org.junit.Assert.assertEquals;

import org.jets3t.service.acl.CanonicalGrantee;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.model.StorageOwner;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link GCSUtils} methods.
 */
public final class GCSUtilsTest {

  private static final String NAME = "foo";
  private static final String ID = "123456789012";
  private static final String OTHER_ID = "987654321098";

  private CanonicalGrantee mUserGrantee;
  private GSAccessControlList mAcl;

  @Before
  public void before() throws Exception {
    // Setup owner.
    mUserGrantee = new CanonicalGrantee(ID);
    mUserGrantee.setDisplayName(NAME);

    // Setup the acl.
    mAcl = new GSAccessControlList();
    mAcl.setOwner(new StorageOwner(ID, NAME));
  }

  @Test
  public void translateUserReadPermission() {
    mAcl.grantPermission(mUserGrantee, Permission.PERMISSION_READ);
    assertEquals((short) 0500, GCSUtils.translateBucketAcl(mAcl, ID));
    assertEquals((short) 0000, GCSUtils.translateBucketAcl(mAcl, OTHER_ID));
    mAcl.grantPermission(mUserGrantee, Permission.PERMISSION_READ_ACP);
    assertEquals((short) 0500, GCSUtils.translateBucketAcl(mAcl, ID));
    assertEquals((short) 0000, GCSUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateUserWritePermission() {
    mAcl.grantPermission(mUserGrantee, Permission.PERMISSION_WRITE);
    assertEquals((short) 0200, GCSUtils.translateBucketAcl(mAcl, ID));
    mAcl.grantPermission(mUserGrantee, Permission.PERMISSION_READ);
    assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, ID));
  }

  @Test
  public void translateUserFullPermission() {
    mAcl.grantPermission(mUserGrantee, Permission.PERMISSION_FULL_CONTROL);
    assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, ID));
    assertEquals((short) 0000, GCSUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateEveryoneReadPermission() {
    GroupGrantee allUsersGrantee = GroupGrantee.ALL_USERS;
    mAcl.grantPermission(allUsersGrantee, Permission.PERMISSION_READ);
    assertEquals((short) 0500, GCSUtils.translateBucketAcl(mAcl, ID));
    assertEquals((short) 0500, GCSUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateEveryoneWritePermission() {
    GroupGrantee allUsersGrantee = GroupGrantee.ALL_USERS;
    mAcl.grantPermission(allUsersGrantee, Permission.PERMISSION_WRITE);
    assertEquals((short) 0200, GCSUtils.translateBucketAcl(mAcl, ID));
    assertEquals((short) 0200, GCSUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateEveryoneFullPermission() {
    GroupGrantee allUsersGrantee = GroupGrantee.ALL_USERS;
    mAcl.grantPermission(allUsersGrantee, Permission.PERMISSION_FULL_CONTROL);
    assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, ID));
    assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateAuthenticatedUserReadPermission() {
    GroupGrantee authenticatedUsersGrantee = GroupGrantee.AUTHENTICATED_USERS;
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_READ);
    assertEquals((short) 0500, GCSUtils.translateBucketAcl(mAcl, ID));
    assertEquals((short) 0500, GCSUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateAuthenticatedUserWritePermission() {
    GroupGrantee authenticatedUsersGrantee = GroupGrantee.AUTHENTICATED_USERS;
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_WRITE);
    assertEquals((short) 0200, GCSUtils.translateBucketAcl(mAcl, ID));
    assertEquals((short) 0200, GCSUtils.translateBucketAcl(mAcl, OTHER_ID));
  }

  @Test
  public void translateAuthenticatedUserFullPermission() {
    GroupGrantee authenticatedUsersGrantee = GroupGrantee.AUTHENTICATED_USERS;
    mAcl.grantPermission(authenticatedUsersGrantee, Permission.PERMISSION_FULL_CONTROL);
    assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, ID));
    assertEquals((short) 0700, GCSUtils.translateBucketAcl(mAcl, OTHER_ID));
  }
}
