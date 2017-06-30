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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.group.provider.IdentityUserGroupsMapping;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Tests for chown command.
 */
public final class ChownCommandTest extends AbstractAlluxioShellTest {
  private void checkFileOwnerAndGroup(String path, String expectedOwner, String expectedGroup)
      throws Exception {
    String currentOwner = mFileSystem.getStatus(new AlluxioURI(path)).getOwner();
    String currentGroup = mFileSystem.getStatus(new AlluxioURI(path)).getGroup();
    Assert.assertEquals(expectedOwner, currentOwner);
    Assert.assertEquals(expectedGroup, currentGroup);
  }

  private void setupTestGroupMappingService() {
    clearLoginUser();
    GroupMappingServiceTestUtils.resetCache();
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
  }

  @Test
  public void chown() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chown", "user1", "/testFile");
    String owner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    Assert.assertEquals("user1", owner);
    mFsShell.run("chown", "user2", "/testFile");
    owner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    Assert.assertEquals("user2", owner);
  }

  @Test
  public void chownValidOwnerValidGroup() throws Exception {
    setupTestGroupMappingService();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String newOwner = "alice";
    List<String> groups = CommonUtils.getGroups(newOwner);
    Random rand = new Random();
    int numOfGroups = groups.size();
    for (int i = 0; i < 8; i++) {
      int k = rand.nextInt(numOfGroups);
      String group = groups.get(k);
      mFsShell.run("chown", newOwner + ":" + group, "/testFile");
      checkFileOwnerAndGroup("/testFile", newOwner, group);
    }
  }

  @Test
  public void chownInvalidOwnerValidGroup() throws Exception {
    setupTestGroupMappingService();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String newOwner = "alice";
    String nonexistUser = "nonexistuser";
    List<String> groups = CommonUtils.getGroups(newOwner);
    int numOfGroups = groups.size();
    Random rand = new Random();
    String originalOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    for (int i = 0; i < 8; i++) {
      int k = rand.nextInt(numOfGroups);
      String group = groups.get(k);
      mFsShell.run("chown", nonexistUser + ":" + group, "/testFile");
      checkFileOwnerAndGroup("/testFile", originalOwner, originalGroup);
    }
  }

  @Test
  public void chownValidOwnerInvalidGroup() throws Exception {
    setupTestGroupMappingService();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String newOwner = "alice";
    String nonexistGroup = "nonexistgroup";
    String originalOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    mFsShell.run("chown", newOwner + ":" + nonexistGroup, "/testFile");
    checkFileOwnerAndGroup("/testFile", originalOwner, originalGroup);
  }

  @Test
  public void chownInvalidOwnerInvalidGroup() throws Exception {
    setupTestGroupMappingService();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String nonexistUser = "nonexistuser";
    String nonexistGroup = "nonexistgroup";
    String originalOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    mFsShell.run("chown", nonexistUser + ":" + nonexistGroup, "/testFile");
    checkFileOwnerAndGroup("/testFile", originalOwner, originalGroup);
  }

  /**
   * Tests -R option for chown recursively.
   */
  @Test
  public void chownRecursive() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testDir/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chown", "-R", "user1", "/testDir");
    String owner = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getOwner();
    Assert.assertEquals("user1", owner);
    owner = mFileSystem.getStatus(new AlluxioURI("/testDir")).getOwner();
    Assert.assertEquals("user1", owner);
    mFsShell.run("chown", "-R", "user2", "/testDir");
    owner = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getOwner();
    Assert.assertEquals("user2", owner);
  }
}
