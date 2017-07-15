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
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.security.group.GroupMappingService;
import alluxio.shell.AbstractAlluxioShellTest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Tests for chown command.
 */
public final class ChownCommandIntegrationTest extends AbstractAlluxioShellTest {
  /*
   * The user and group mappings for testing are:
   *    alice -> alice,staff
   *    bob   -> bob,staff
   */
  private static final TestUser TEST_USER_1 =
      new TestUser("alice", "alice,staff");
  private static final TestUser TEST_USER_2 =
      new TestUser("bob", "bob,staff");

  @Rule
  public ConfigurationRule mConfiguration = new ConfigurationRule(ImmutableMap
      .of(PropertyKey.SECURITY_GROUP_MAPPING_CLASS, FakeUserGroupsMapping.class.getName()));

  /**
   * A simple structure to represent a user and its groups.
   */
  private static final class TestUser {
    private String mUser;
    private String mGroup;

    TestUser(String user, String group) {
      mUser = user;
      mGroup = group;
    }

    String getUser() {
      return mUser;
    }

    String getGroup() {
      return mGroup;
    }
  }

  /**
   * Test class implements {@link GroupMappingService} providing user-to-groups mapping.
   */
  public static class FakeUserGroupsMapping implements GroupMappingService {
    private HashMap<String, String> mUserGroups = new HashMap<>();

    /**
     * Constructor of {@link FakeUserGroupsMapping} to put the user and groups in user-to-groups
     * HashMap.
     */
    public FakeUserGroupsMapping() {
      mUserGroups.put(TEST_USER_1.getUser(), TEST_USER_1.getGroup());
      mUserGroups.put(TEST_USER_2.getUser(), TEST_USER_2.getGroup());
    }

    @Override
    public List<String> getGroups(String user) throws IOException {
      if (mUserGroups.containsKey(user)) {
        return Lists.newArrayList(mUserGroups.get(user).split(","));
      }
      return new ArrayList<>();
    }
  }

  @Test
  public void chown() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chown", TEST_USER_1.getUser(), "/testFile");
    String owner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    Assert.assertEquals(TEST_USER_1.getUser(), owner);
    mFsShell.run("chown", TEST_USER_2.getUser(), "/testFile");
    owner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    Assert.assertEquals(TEST_USER_2.getUser(), owner);
  }

  @Test
  public void chownValidOwnerValidGroupSuccess() throws Exception {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
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
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String newOwner = TEST_USER_2.getUser();
    String originalOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    String group = "alice";
    String expectedCommandOutput =
        String.format("Could not update owner:group for /testFile to %s:%s", newOwner, group);
    verifyCommandReturnValueAndOutput(-1, expectedCommandOutput,
        "chown", newOwner + ":" + group, "/testFile");
    checkPathOwnerAndGroup("/testFile", originalOwner, originalGroup);
  }

  @Test
  public void chownInvalidOwnerValidGroup() throws Exception {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String nonexistUser = "nonexistuser";
    String originalOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    String group = "staff";
    String expectedCommandOutput =
        String.format("Could not update owner:group for /testFile to %s:%s", nonexistUser, group);
    verifyCommandReturnValueAndOutput(-1, expectedCommandOutput,
        "chown", nonexistUser + ":" + group, "/testFile");
    checkPathOwnerAndGroup("/testFile", originalOwner, originalGroup);
  }

  @Test
  public void chownValidOwnerInvalidGroup() throws Exception {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String newOwner = TEST_USER_1.getUser();
    String nonexistGroup = "nonexistgroup";
    String originalOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    String expectedCommandOutput =
        String.format("Could not update owner:group for /testFile to %s:%s",
        newOwner, nonexistGroup);
    verifyCommandReturnValueAndOutput(-1, expectedCommandOutput,
        "chown", newOwner + ":" + nonexistGroup, "/testFile");
    checkPathOwnerAndGroup("/testFile", originalOwner, originalGroup);
  }

  @Test
  public void chownInvalidOwnerInvalidGroup() throws Exception {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    String nonexistUser = "nonexistuser";
    String nonexistGroup = "nonexistgroup";
    String originalOwner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getOwner();
    String originalGroup = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroup();
    String expectedCommandOutput =
        String.format("Could not update owner:group for /testFile to %s:%s",
        nonexistUser, nonexistGroup);
    verifyCommandReturnValueAndOutput(-1, expectedCommandOutput,
        "chown", nonexistUser + ":" + nonexistGroup, "/testFile");
    checkPathOwnerAndGroup("/testFile", originalOwner, originalGroup);
  }

  /**
   * Tests -R option for chown recursively.
   */
  @Test
  public void chownRecursive() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testDir/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chown", "-R", TEST_USER_1.getUser(), "/testDir");
    String owner = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getOwner();
    Assert.assertEquals(TEST_USER_1.getUser(), owner);
    owner = mFileSystem.getStatus(new AlluxioURI("/testDir")).getOwner();
    Assert.assertEquals(TEST_USER_1.getUser(), owner);
    mFsShell.run("chown", "-R", TEST_USER_2.getUser(), "/testDir");
    owner = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getOwner();
    Assert.assertEquals(TEST_USER_2.getUser(), owner);
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
    String currentOwner = mFileSystem.getStatus(new AlluxioURI(path)).getOwner();
    String currentGroup = mFileSystem.getStatus(new AlluxioURI(path)).getGroup();
    Assert.assertEquals(expectedOwner, currentOwner);
    Assert.assertEquals(expectedGroup, currentGroup);
  }
}
