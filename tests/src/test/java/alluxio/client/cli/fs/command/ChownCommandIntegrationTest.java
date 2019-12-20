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

package alluxio.client.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.WritePType;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for chown command.
 */
public final class ChownCommandIntegrationTest extends AbstractFileSystemShellTest {

  @Rule
  public ConfigurationRule mConfiguration = new ConfigurationRule(ImmutableMap
      .of(PropertyKey.SECURITY_GROUP_MAPPING_CLASS, FakeUserGroupsMapping.class.getName()),
      ServerConfiguration.global());

  @Test
  public void chown() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    sFsShell.run("chown", TEST_USER_1.getUser(), "/testFile");
    String owner = sFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    Assert.assertEquals(TEST_USER_1.getUser(), owner);
    sFsShell.run("chown", TEST_USER_2.getUser(), "/testFile");
    owner = sFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    Assert.assertEquals(TEST_USER_2.getUser(), owner);
  }

  @Test
  public void chownValidOwnerValidGroupSuccess() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    String newOwner = TEST_USER_1.getUser();
    String group = "staff";
    String expectedCommandOutput =
        "Changed owner:group of /testFile to " + newOwner + ":" +  group + ".";
    verifyCommandReturnValueAndOutput(0, expectedCommandOutput,
        "chown", newOwner + ":" + group, "/testFile");
    checkPathOwnerAndGroup("/testFile", newOwner, group);
  }

  @Test
  public void chownValidOwnerValidGroupFail() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    String newOwner = TEST_USER_2.getUser();
    String originalOwner = sFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = sFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    String group = "alice";
    String expectedCommandOutput =
        String.format("Could not update owner:group for /testFile to %s:%s", newOwner, group);
    verifyCommandReturnValueAndOutput(-1, expectedCommandOutput,
        "chown", newOwner + ":" + group, "/testFile");
    checkPathOwnerAndGroup("/testFile", originalOwner, originalGroup);
  }

  @Test
  public void chownInvalidOwnerValidGroup() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    String nonexistUser = "nonexistuser";
    String originalOwner = sFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = sFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    String group = "staff";
    String expectedCommandOutput =
        String.format("Could not update owner:group for /testFile to %s:%s", nonexistUser, group);
    verifyCommandReturnValueAndOutput(-1, expectedCommandOutput,
        "chown", nonexistUser + ":" + group, "/testFile");
    checkPathOwnerAndGroup("/testFile", originalOwner, originalGroup);
  }

  @Test
  public void chownValidOwnerInvalidGroup() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    String newOwner = TEST_USER_1.getUser();
    String nonexistGroup = "nonexistgroup";
    String originalOwner = sFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = sFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    String expectedCommandOutput =
        String.format("Could not update owner:group for /testFile to %s:%s",
        newOwner, nonexistGroup);
    verifyCommandReturnValueAndOutput(-1, expectedCommandOutput,
        "chown", newOwner + ":" + nonexistGroup, "/testFile");
    checkPathOwnerAndGroup("/testFile", originalOwner, originalGroup);
  }

  @Test
  public void chownInvalidOwnerInvalidGroup() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    String nonexistUser = "nonexistuser";
    String nonexistGroup = "nonexistgroup";
    String originalOwner = sFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = sFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    String expectedCommandOutput =
        String.format("Could not update owner:group for /testFile to %s:%s",
        nonexistUser, nonexistGroup);
    verifyCommandReturnValueAndOutput(-1, expectedCommandOutput,
        "chown", nonexistUser + ":" + nonexistGroup, "/testFile");
    checkPathOwnerAndGroup("/testFile", originalOwner, originalGroup);
  }

  /**
   * Tests -R option for chown.
   */
  @Test
  public void chownRecursive() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testDir/testFile",
        WritePType.MUST_CACHE, 10);
    sFsShell.run("chown", "-R", TEST_USER_1.getUser(), "/testDir");
    String owner = sFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getOwner();
    Assert.assertEquals(TEST_USER_1.getUser(), owner);
    owner = sFileSystem.getStatus(new AlluxioURI("/testDir")).getOwner();
    Assert.assertEquals(TEST_USER_1.getUser(), owner);
    sFsShell.run("chown", "-R", TEST_USER_2.getUser(), "/testDir");
    owner = sFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getOwner();
    Assert.assertEquals(TEST_USER_2.getUser(), owner);
  }

  /**
   * Tests chown with wildcard entries.
   */
  @Test
  public void chownWildcard() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testDir/testFile1",
        WritePType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testDir2/testFile2",
        WritePType.MUST_CACHE, 10);

    sFsShell.run("chown", "-R", TEST_USER_1.getUser(), "/*/testFile*");
    String owner = sFileSystem.getStatus(new AlluxioURI("/testDir/testFile1")).getOwner();
    Assert.assertEquals(TEST_USER_1.getUser(), owner);
    owner = sFileSystem.getStatus(new AlluxioURI("/testDir2/testFile2")).getOwner();
    Assert.assertEquals(TEST_USER_1.getUser(), owner);
  }

  /**
   * Checks whether the owner and group of the path are expectedOwner and expectedGroup.
   *
   * @param path the path to check
   * @param expectedOwner the owner that we expect to own the path
   * @param expectedGroup the expected group of the path
   */
  private void checkPathOwnerAndGroup(String path, String expectedOwner, String expectedGroup)
      throws Exception {
    String currentOwner = sFileSystem.getStatus(new AlluxioURI(path)).getOwner();
    String currentGroup = sFileSystem.getStatus(new AlluxioURI(path)).getGroup();
    Assert.assertEquals(expectedOwner, currentOwner);
    Assert.assertEquals(expectedGroup, currentGroup);
  }
}
