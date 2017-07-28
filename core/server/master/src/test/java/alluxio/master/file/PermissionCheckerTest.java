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
import alluxio.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.master.MasterRegistry;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.NoopJournalContext;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.security.group.GroupMappingService;
import alluxio.underfs.UfsManager;

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
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
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

  private static final Mode TEST_NORMAL_MODE = new Mode((short) 0755);
  private static final Mode TEST_WEIRD_MODE = new Mode((short) 0157);

  private static CreateFileOptions sFileOptions;
  private static CreateFileOptions sWeirdFileOptions;
  private static CreateFileOptions sNestedFileOptions;

  private static InodeTree sTree;
  private static MasterRegistry sRegistry;

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
      mUserGroups.put(TEST_USER_ADMIN.getUser(), TEST_USER_ADMIN.getGroup());
      mUserGroups.put(TEST_USER_1.getUser(), TEST_USER_1.getGroup());
      mUserGroups.put(TEST_USER_2.getUser(), TEST_USER_2.getGroup());
      mUserGroups.put(TEST_USER_3.getUser(), TEST_USER_3.getGroup());
      mUserGroups.put(TEST_USER_SUPERGROUP.getUser(), TEST_USER_SUPERGROUP.getGroup());
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
    sFileOptions =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setOwner(TEST_USER_2.getUser())
            .setGroup(TEST_USER_2.getGroup()).setMode(TEST_NORMAL_MODE);
    sWeirdFileOptions =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setOwner(TEST_USER_1.getUser())
            .setGroup(TEST_USER_1.getGroup()).setMode(TEST_WEIRD_MODE);
    sNestedFileOptions =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setOwner(TEST_USER_1.getUser())
            .setGroup(TEST_USER_1.getGroup()).setMode(TEST_NORMAL_MODE).setRecursive(true);

    // setup an InodeTree
    sRegistry = new MasterRegistry();
    JournalFactory factory =
        new Journal.Factory(new URI(sTestFolder.newFolder().getAbsolutePath()));

    BlockMaster blockMaster = new BlockMasterFactory().create(sRegistry, factory);
    InodeDirectoryIdGenerator directoryIdGenerator = new InodeDirectoryIdGenerator(blockMaster);
    UfsManager ufsManager = Mockito.mock(UfsManager.class);
    MountTable mountTable = new MountTable(ufsManager);
    sTree = new InodeTree(blockMaster, directoryIdGenerator, mountTable);

    sRegistry.start(true);

    GroupMappingServiceTestUtils.resetCache();
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        FakeUserGroupsMapping.class.getName());
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, TEST_SUPER_GROUP);
    sTree.initializeRoot(TEST_USER_ADMIN.getUser(), TEST_USER_ADMIN.getGroup(), TEST_NORMAL_MODE);

    // build file structure
    createAndSetPermission(TEST_DIR_FILE_URI, sNestedFileOptions);
    createAndSetPermission(TEST_FILE_URI, sFileOptions);
    createAndSetPermission(TEST_WEIRD_FILE_URI, sWeirdFileOptions);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    sRegistry.stop();
    AuthenticatedClientUser.remove();
    ConfigurationTestUtils.resetConfiguration();
  }

  @Before
  public void before() throws Exception {
    AuthenticatedClientUser.remove();
    mPermissionChecker = new PermissionChecker(sTree);
  }

  /**
   * Helper function to create a path and set the permission to what specified in option.
   *
   * @param path path to construct the {@link AlluxioURI} from
   * @param option method options for creating a file
   */
  private static void createAndSetPermission(String path, CreateFileOptions option)
      throws Exception {
    try (
        LockedInodePath inodePath = sTree
            .lockInodePath(new AlluxioURI(path), InodeTree.LockMode.WRITE)) {
      InodeTree.CreatePathResult result =
          sTree.createPath(inodePath, option, new NoopJournalContext());
      ((InodeFile) result.getCreated().get(result.getCreated().size() - 1))
          .setOwner(option.getOwner()).setGroup(option.getGroup())
          .setMode(option.getMode().toShort());
    }
  }

  /**
   * Verifies that the list of inodes are same as the expected ones.
   * @param expectedInodes the expected inodes names
   * @param inodes the inodes for test
   */
  private static void verifyInodesList(String[] expectedInodes, List<Inode<?>> inodes) {
    String[] inodesName = new String[inodes.size()];
    for (int i = 0; i < inodes.size(); i++) {
      inodesName[i] = inodes.get(i).getName();
    }

    Assert.assertArrayEquals(expectedInodes, inodesName);
  }

  @Test
  public void createFileAndDirs() throws Exception {
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
  public void fileSystemOwner() throws Exception {
    checkPermission(TEST_USER_ADMIN, Mode.Bits.ALL, TEST_DIR_FILE_URI);
    checkPermission(TEST_USER_ADMIN, Mode.Bits.ALL, TEST_DIR_URI);
    checkPermission(TEST_USER_ADMIN, Mode.Bits.ALL, TEST_FILE_URI);
  }

  @Test
  public void fileSystemSuperGroup() throws Exception {
    checkPermission(TEST_USER_SUPERGROUP, Mode.Bits.ALL, TEST_DIR_FILE_URI);
    checkPermission(TEST_USER_SUPERGROUP, Mode.Bits.ALL, TEST_DIR_URI);
    checkPermission(TEST_USER_SUPERGROUP, Mode.Bits.ALL, TEST_FILE_URI);
  }

  @Test
  public void selfCheckSuccess() throws Exception {
    // the same owner
    checkPermission(TEST_USER_1, Mode.Bits.READ, TEST_DIR_FILE_URI);
    checkPermission(TEST_USER_1, Mode.Bits.WRITE, TEST_DIR_FILE_URI);

    // not the owner and in other group
    checkPermission(TEST_USER_2, Mode.Bits.READ, TEST_DIR_FILE_URI);

    // not the owner but in same group
    checkPermission(TEST_USER_3, Mode.Bits.READ, TEST_DIR_FILE_URI);
  }

  @Test
  public void selfCheckFailByOtherGroup() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_FILE_URI,
            "file")));

    // not the owner and in other group
    checkPermission(TEST_USER_2, Mode.Bits.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void selfCheckFailBySameGroup() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_3.getUser(), Mode.Bits.WRITE, TEST_DIR_FILE_URI,
            "file")));

    // not the owner but in same group
    checkPermission(TEST_USER_3, Mode.Bits.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void checkFallThrough() throws Exception {
    // user can not read, but group can
    checkPermission(TEST_USER_1, Mode.Bits.READ, TEST_WEIRD_FILE_URI);

    // user and group can not write, but other can
    checkPermission(TEST_USER_1, Mode.Bits.WRITE, TEST_WEIRD_FILE_URI);
  }

  @Test
  public void parentCheckSuccess() throws Exception {
    checkParentOrAncestorPermission(TEST_USER_1, Mode.Bits.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void parentCheckFail() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_FILE_URI,
            "testDir")));

    checkParentOrAncestorPermission(TEST_USER_2, Mode.Bits.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void ancestorCheckSuccess() throws Exception {
    checkParentOrAncestorPermission(TEST_USER_1, Mode.Bits.WRITE, TEST_NOT_EXIST_URI);
  }

  @Test
  public void ancestorCheckFail() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_NOT_EXIST_URI,
            "testDir")));

    checkParentOrAncestorPermission(TEST_USER_2, Mode.Bits.WRITE, TEST_NOT_EXIST_URI);
  }

  @Test
  public void invalidPath() throws Exception {
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

  /**
   * Helper function to check a user has permission to perform a action on the parent or ancestor of
   * the given path.
   *
   * @param user a user with groups
   * @param action action that capture the action {@link Mode.Bits} by user
   * @param path path to construct the {@link AlluxioURI} from
   */
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
