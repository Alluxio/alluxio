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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.master.MasterContext;
import alluxio.master.MasterSource;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.security.authorization.Permission;
import alluxio.security.group.GroupMappingService;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Unit tests for {@link PermissionChecker}.
 */
public final class PermissionCheckerTest {
  private static final String TEST_SUPER_GROUP = "test-supergroup";

  /*
   * The user and group mappings for testing are:
   *    admin -> admin
   *    user1 -> group1
   *    user2 -> group2
   *    user3 -> group1
   *    user4 -> group2,test-supergroup
   */
  private static final TestUser TEST_USER_ADMIN = new TestUser("admin", "admin");
  private static final TestUser TEST_USER_1 = new TestUser("user1", "group1");
  private static final TestUser TEST_USER_2 = new TestUser("user2", "group2");
  private static final TestUser TEST_USER_3 = new TestUser("user3", "group1");
  private static final TestUser TEST_USER_SUPERGROUP =
      new TestUser("user4", "group2,test-supergroup");

  /*
   * The file structure for testing is:
   *    /               admin     admin       755
   *    /testDir        user1     group1      755
   *    /testDir/file   user1     group1      644
   *    /testFile       user2     group2      644
   *    /testWeirdFile  user1     group1      046
   */
  private static final String TEST_DIR_URI = "/testDir";
  private static final String TEST_DIR_FILE_URI = "/testDir/file";
  private static final String TEST_FILE_URI = "/testFile";
  private static final String TEST_NOT_EXIST_URI = "/testDir/notExistDir/notExistFile";
  private static final String TEST_WEIRD_FILE_URI = "/testWeirdFile";

  private static final Permission TEST_PERMISSION_SUPER =
      new Permission(TEST_USER_ADMIN.getUser(), TEST_USER_ADMIN.getGroups(), (short) 0755);
  private static final Permission TEST_PERMISSION_1 =
      new Permission(TEST_USER_1.getUser(), TEST_USER_1.getGroups(), (short) 0755);
  private static final Permission TEST_PERMISSION_2 =
      new Permission(TEST_USER_2.getUser(), TEST_USER_2.getGroups(), (short) 0755);
  private static final Permission TEST_PERMISSION_WEIRD =
      new Permission(TEST_USER_1.getUser(), TEST_USER_1.getGroups(), (short) 0157);

  private static CreateFileOptions sFileOptions;
  private static CreateFileOptions sWeirdFileOptions;
  private static CreateFileOptions sNestedFileOptions;

  private static InodeTree sTree;

  private PermissionChecker mPermissionChecker;

  @ClassRule
  public static TemporaryFolder sTestFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * A simple structure to represent a user and its groups.
   */
  private static final class TestUser {
    private String mUser;
    private String mGroups;

    TestUser(String user, String groups) {
      mUser = user;
      mGroups = groups;
    }

    String getUser() {
      return mUser;
    }

    String getGroups() {
      return mGroups;
    }
  }

  public static class FakeUserGroupsMapping implements GroupMappingService {
    private HashMap<String, String> mUserGroups = new HashMap<>();

    public FakeUserGroupsMapping() {
      mUserGroups.put(TEST_USER_ADMIN.getUser(), TEST_USER_ADMIN.getGroups());
      mUserGroups.put(TEST_USER_1.getUser(), TEST_USER_1.getGroups());
      mUserGroups.put(TEST_USER_2.getUser(), TEST_USER_2.getGroups());
      mUserGroups.put(TEST_USER_3.getUser(), TEST_USER_3.getGroups());
      mUserGroups.put(TEST_USER_SUPERGROUP.getUser(), TEST_USER_SUPERGROUP.getGroups());
    }

    @Override
    public List<String> getGroups(String user) throws IOException {
      if (mUserGroups.containsKey(user)) {
        return Lists.newArrayList(mUserGroups.get(user).split(","));
      }
      return new ArrayList<>();
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    sFileOptions = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
        .setPermission(TEST_PERMISSION_2);
    sWeirdFileOptions = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
        .setPermission(TEST_PERMISSION_WEIRD);
    sNestedFileOptions = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
        .setPermission(TEST_PERMISSION_1).setRecursive(true);

    // setup an InodeTree
    Journal blockJournal = new ReadWriteJournal(sTestFolder.newFolder().getAbsolutePath());

    BlockMaster blockMaster = new BlockMaster(new MasterContext(new MasterSource()), blockJournal);
    InodeDirectoryIdGenerator directoryIdGenerator = new InodeDirectoryIdGenerator(blockMaster);
    MountTable mountTable = new MountTable();
    sTree = new InodeTree(blockMaster, directoryIdGenerator, mountTable);

    blockMaster.start(true);

    Configuration.set(Constants.SECURITY_GROUP_MAPPING_CLASS,
        FakeUserGroupsMapping.class.getName());
    Configuration.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    Configuration.set(Constants.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, TEST_SUPER_GROUP);
    sTree.initializeRoot(TEST_PERMISSION_SUPER);

    // build file structure
    createAndSetPermission(TEST_DIR_FILE_URI, sNestedFileOptions);
    createAndSetPermission(TEST_FILE_URI, sFileOptions);
    createAndSetPermission(TEST_WEIRD_FILE_URI, sWeirdFileOptions);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Before
  public void before() throws Exception {
    mPermissionChecker = new PermissionChecker(sTree);
  }

  // Helper function to create a path and set the permission to what specified in option.
  private static void createAndSetPermission(String path, CreateFileOptions option)
      throws Exception {
    try (
        LockedInodePath inodePath = sTree
            .lockInodePath(new AlluxioURI(path), InodeTree.LockMode.WRITE)) {
      InodeTree.CreatePathResult result = sTree.createPath(inodePath, option);
      result.getCreated().get(result.getCreated().size() - 1)
          .setPermission(option.getPermission());
    }
  }

  private static void verifyInodesList(String[] expectedInodes, List<Inode<?>> inodes) {
    String[] inodesName = new String[inodes.size()];
    for (int i = 0; i < inodes.size(); i++) {
      inodesName[i] = inodes.get(i).getName();
    }

    Assert.assertArrayEquals(expectedInodes, inodesName);
  }

  @Test
  public void createFileAndDirsTest() throws Exception {
    try (LockedInodePath inodePath = sTree.lockInodePath(new AlluxioURI(TEST_DIR_FILE_URI),
        InodeTree.LockMode.READ)) {
      verifyInodesList(TEST_DIR_FILE_URI.split("/"), inodePath.getInodeList());
    }
    try (LockedInodePath inodePath = sTree.lockInodePath(new AlluxioURI(TEST_FILE_URI),
        InodeTree.LockMode.READ)) {
      verifyInodesList(TEST_FILE_URI.split("/"), inodePath.getInodeList());
    }
    try (LockedInodePath inodePath = sTree.lockInodePath(new AlluxioURI(TEST_WEIRD_FILE_URI),
        InodeTree.LockMode.READ)) {
      verifyInodesList(TEST_WEIRD_FILE_URI.split("/"), inodePath.getInodeList());
    }
    try (LockedInodePath inodePath = sTree.lockInodePath(new AlluxioURI(TEST_NOT_EXIST_URI),
        InodeTree.LockMode.READ)) {
      verifyInodesList(new String[]{"", "testDir"}, inodePath.getInodeList());
    }
  }

  @Test
  public void fileSystemOwnerTest() throws Exception {
    checkPermission(TEST_USER_ADMIN, Mode.Bits.ALL, TEST_DIR_FILE_URI);
    checkPermission(TEST_USER_ADMIN, Mode.Bits.ALL, TEST_DIR_URI);
    checkPermission(TEST_USER_ADMIN, Mode.Bits.ALL, TEST_FILE_URI);
  }

  @Test
  public void fileSystemSuperGroupTest() throws Exception {
    checkPermission(TEST_USER_SUPERGROUP, Mode.Bits.ALL, TEST_DIR_FILE_URI);
    checkPermission(TEST_USER_SUPERGROUP, Mode.Bits.ALL, TEST_DIR_URI);
    checkPermission(TEST_USER_SUPERGROUP, Mode.Bits.ALL, TEST_FILE_URI);
  }

  @Test
  public void selfCheckSuccessTest() throws Exception {
    // the same owner
    checkPermission(TEST_USER_1, Mode.Bits.READ, TEST_DIR_FILE_URI);
    checkPermission(TEST_USER_1, Mode.Bits.WRITE, TEST_DIR_FILE_URI);

    // not the owner and in other group
    checkPermission(TEST_USER_2, Mode.Bits.READ, TEST_DIR_FILE_URI);

    // not the owner but in same group
    checkPermission(TEST_USER_3, Mode.Bits.READ, TEST_DIR_FILE_URI);
  }

  @Test
  public void selfCheckFailByOtherGroupTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_FILE_URI,
            "file")));

    // not the owner and in other group
    checkPermission(TEST_USER_2, Mode.Bits.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void selfCheckFailBySameGroupTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_3.getUser(), Mode.Bits.WRITE, TEST_DIR_FILE_URI,
            "file")));

    // not the owner but in same group
    checkPermission(TEST_USER_3, Mode.Bits.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void checkFallThroughTest() throws Exception {
    // user can not read, but group can
    checkPermission(TEST_USER_1, Mode.Bits.READ, TEST_WEIRD_FILE_URI);

    // user and group can not write, but other can
    checkPermission(TEST_USER_1, Mode.Bits.WRITE, TEST_WEIRD_FILE_URI);
  }

  @Test
  public void parentCheckSuccessTest() throws Exception {
    checkParentOrAncestorPermission(TEST_USER_1, Mode.Bits.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void parentCheckFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_FILE_URI,
            "testDir")));

    checkParentOrAncestorPermission(TEST_USER_2, Mode.Bits.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void ancestorCheckSuccessTest() throws Exception {
    checkParentOrAncestorPermission(TEST_USER_1, Mode.Bits.WRITE, TEST_NOT_EXIST_URI);
  }

  @Test
  public void ancestorCheckFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_NOT_EXIST_URI,
            "testDir")));

    checkParentOrAncestorPermission(TEST_USER_2, Mode.Bits.WRITE, TEST_NOT_EXIST_URI);
  }

  @Test
  public void invalidPathTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    try (LockedInodePath inodePath = sTree
        .lockInodePath(new AlluxioURI(""), InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(Mode.Bits.WRITE, inodePath);
    }
  }

  /**
   * Helper function to check user can perform action on path.
   */
  private void checkPermission(TestUser user, Mode.Bits action, String path)
      throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    try (LockedInodePath inodePath = sTree
        .lockInodePath(new AlluxioURI(path), InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(action, inodePath);
    }
  }

  private void checkParentOrAncestorPermission(TestUser user, Mode.Bits action, String path)
      throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    try (LockedInodePath inodePath = sTree
        .lockInodePath(new AlluxioURI(path), InodeTree.LockMode.READ)) {
      mPermissionChecker.checkParentPermission(action, inodePath);
    }
  }

  private String toExceptionMessage(String user, Mode.Bits action, String path,
      String inodeName) {
    StringBuilder stringBuilder = new StringBuilder()
        .append("user=").append(user).append(", ")
        .append("access=").append(action).append(", ")
        .append("path=").append(path).append(": ")
        .append("failed at ")
        .append(inodeName);
    return stringBuilder.toString();
  }
}
