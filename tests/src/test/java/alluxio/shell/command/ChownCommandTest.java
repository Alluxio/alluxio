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
  public void chownUserAndGroup() throws Exception {
    clearLoginUser();
    GroupMappingServiceTestUtils.resetCache();
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");

    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String newOwner = "alice";
    String nonexistUser = "nonexistuser";
    List<String> groups = CommonUtils.getGroups(newOwner);
    String nonexistGroup = "nonexistgroup";
    Random rand = new Random();
    int numOfGroups = groups.size();
    // chown legalUser:legalGroup
    for (int i = 0; i < 8; i++) {
      int k = rand.nextInt(numOfGroups);
      String group = groups.get(k);
      mFsShell.run("chown", newOwner + ":" + group, "/testFile");
      String currentGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
      Assert.assertEquals(group, currentGroup);
    }

    String originalOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    // chown illegalUser:legalGroup
    for (int i = 0; i < 8; i++) {
      int k = rand.nextInt(numOfGroups);
      String group = groups.get(k);
      mFsShell.run("chown", nonexistUser + ":" + group, "/testFile");
      String currentOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
      String currentGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
      Assert.assertEquals(originalOwner, currentOwner);
      Assert.assertEquals(originalGroup, currentGroup);
    }

    // chown illegalUser:illegalGroup
    mFsShell.run("chown", nonexistUser + ":" + nonexistGroup, "/testFile");
    do {
      String currentOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
      String currentGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
      Assert.assertEquals(originalOwner, currentOwner);
      Assert.assertEquals(originalGroup, currentGroup);
    } while (false);

    // chown legalUser:illegalGroup
    mFsShell.run("chown", newOwner + ":" + nonexistGroup, "/testFile");
    do {
      String currentOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
      String currentGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
      Assert.assertEquals(originalOwner, currentOwner);
      Assert.assertEquals(originalGroup, currentGroup);
    } while (false);
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
