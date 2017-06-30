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
  public void chownValidOwnerValidGroup() throws Exception {
    setupTestGroupMappingService();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String newOwner = "alice";
    List<String> groups = CommonUtils.getGroups(newOwner);
    String group = groups.get(0);
    String expectedCommandOutput =
        "Changed owner:group of /testFile to alice:" +  group + ".";
    runChownOwnerAndGroup("/testFile", 0, expectedCommandOutput, newOwner, group,
        "chown", newOwner + ":" + group, "/testFile");
  }

  @Test
  public void chownInvalidOwnerValidGroup() throws Exception {
    setupTestGroupMappingService();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String user = "alice";
    String nonexistUser = "nonexistuser";
    List<String> groups = CommonUtils.getGroups(user);
    String originalOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    String group = groups.get(0);
    String expectedCommandOutput = "Could not setOwner for /testFile.";
    runChownOwnerAndGroup("/testFile", -1, expectedCommandOutput, originalOwner, originalGroup,
        "chown", nonexistUser + ":" + group, "/testFile");
  }

  @Test
  public void chownValidOwnerInvalidGroup() throws Exception {
    setupTestGroupMappingService();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String newOwner = "alice";
    String nonexistGroup = "nonexistgroup";
    String originalOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    String expectedCommandOutput = "Could not setOwner for /testFile.";
    runChownOwnerAndGroup("/testFile", -1, expectedCommandOutput, originalOwner, originalGroup,
        "chown", newOwner + ":" + nonexistGroup, "/testFile");
  }

  @Test
  public void chownInvalidOwnerInvalidGroup() throws Exception {
    setupTestGroupMappingService();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String nonexistUser = "nonexistuser";
    String nonexistGroup = "nonexistgroup";
    String originalOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    String expectedCommandOutput = "Could not setOwner for /testFile.";
    runChownOwnerAndGroup("/testFile", -1, expectedCommandOutput, originalOwner, originalGroup,
        "chown", nonexistUser + ":" + nonexistGroup, "/testFile");
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

  /**
   * Run chown command to change owner and group of a path.
   *
   * @param command the command to run
   * @param expectedReturnValue return value expected from running the command
   * @param expectedCommandOutput command output expected from running the command
   * @param expectedOwner expected owner of the path
   * @param expectedGroup expected group of the path
   */
  private void runChownOwnerAndGroup(String path, int expectedReturnValue,
      String expectedCommandOutput, String expectedOwner, String expectedGroup, String... command)
      throws Exception {
    verifyCommandReturnValueAndOutput(expectedReturnValue, expectedCommandOutput, command);
    checkPathOwnerAndGroup(path, expectedOwner, expectedGroup);
  }

  /**
   * Check whether the owner and group of the path are expectedOwner and expectedGroup.
   *
   * @param path the path to check
   * @param expectedOwner the owner that we expect to own the path
   * @param expectedGroup the expected group of the path
   */
  private void checkPathOwnerAndGroup(String path, String expectedOwner, String expectedGroup)
      throws Exception {
    String currentOwner = mFileSystem.getStatus(new AlluxioURI(path)).getOwner();
    String currentGroup = mFileSystem.getStatus(new AlluxioURI(path)).getGroup();
    Assert.assertEquals(expectedOwner, currentOwner);
    Assert.assertEquals(expectedGroup, currentGroup);
  }

  /**
   * Set up a group mapping service for test with {@link IdentityUserGroupsMapping}.
   */
  private void setupTestGroupMappingService() {
    clearLoginUser();
    GroupMappingServiceTestUtils.resetCache();
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
  }
}
