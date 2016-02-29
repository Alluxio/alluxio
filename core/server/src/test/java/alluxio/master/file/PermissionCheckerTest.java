/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.Constants;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.master.MasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.security.authorization.FileSystemAction;
import alluxio.security.authorization.PermissionStatus;
import alluxio.wire.FileInfo;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link PermissionChecker}.
 */
public class PermissionCheckerTest {
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

  private static final PermissionStatus TEST_PERMISSION_STATUS_SUPER =
      new PermissionStatus(TEST_USER_ADMIN.getUser(), TEST_USER_ADMIN.getGroups(), (short) 0755);
  private static final PermissionStatus TEST_PERMISSION_STATUS_1 =
      new PermissionStatus(TEST_USER_1.getUser(), TEST_USER_1.getGroups(), (short) 0755);
  private static final PermissionStatus TEST_PERMISSION_STATUS_2 =
      new PermissionStatus(TEST_USER_2.getUser(), TEST_USER_2.getGroups(), (short) 0755);
  private static final PermissionStatus TEST_PERMISSION_STATUS_WEIRD =
      new PermissionStatus(TEST_USER_1.getUser(), TEST_USER_1.getGroups(), (short) 0157);

  private static CreateFileOptions sFileOptions;
  private static CreateFileOptions sWeirdFileOptions;
  private static CreateFileOptions sNestedFileOptions;

  private static InodeTree sTree;

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

  @BeforeClass
  public static void beforeClass() throws Exception {
    sFileOptions = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
        .setPermissionStatus(TEST_PERMISSION_STATUS_2);
    sWeirdFileOptions = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
        .setPermissionStatus(TEST_PERMISSION_STATUS_WEIRD);
    sNestedFileOptions = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
        .setPermissionStatus(TEST_PERMISSION_STATUS_1).setRecursive(true);

    // setup an InodeTree
    Journal blockJournal = new ReadWriteJournal(sTestFolder.newFolder().getAbsolutePath());

    BlockMaster blockMaster = new BlockMaster(blockJournal);
    InodeDirectoryIdGenerator directoryIdGenerator = new InodeDirectoryIdGenerator(blockMaster);
    MountTable mountTable = new MountTable();
    sTree = new InodeTree(blockMaster, directoryIdGenerator, mountTable);

    blockMaster.start(true);

    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, TEST_SUPER_GROUP);
    MasterContext.reset(conf);
    sTree.initializeRoot(TEST_PERMISSION_STATUS_SUPER);

    // verify initialization
    verifyPermissionChecker(true, TEST_PERMISSION_STATUS_SUPER.getUserName(), TEST_SUPER_GROUP);

    // verify initializing root twice
    Inode root = sTree.getInodeByPath(new AlluxioURI("/"));
    sTree.initializeRoot(TEST_PERMISSION_STATUS_SUPER);
    verifyPermissionChecker(true, root.getUserName(), TEST_SUPER_GROUP);

    // build file structure
    createFileAndDirs();
  }

  private static void createFileAndDirs() throws Exception {
    sTree.createPath(new AlluxioURI(TEST_DIR_FILE_URI), sNestedFileOptions);
    sTree.createPath(new AlluxioURI(TEST_FILE_URI), sFileOptions);
    sTree.createPath(new AlluxioURI(TEST_WEIRD_FILE_URI), sWeirdFileOptions);

    verifyInodesList(TEST_DIR_FILE_URI.split("/"),
        sTree.collectInodes(new AlluxioURI(TEST_DIR_FILE_URI)));
    verifyInodesList(TEST_FILE_URI.split("/"),
        sTree.collectInodes(new AlluxioURI(TEST_FILE_URI)));
    verifyInodesList(TEST_WEIRD_FILE_URI.split("/"),
        sTree.collectInodes(new AlluxioURI(TEST_WEIRD_FILE_URI)));
    verifyInodesList(new String[]{"", "testDir"},
        sTree.collectInodes(new AlluxioURI(TEST_NOT_EXIST_URI)));
  }

  private static void verifyInodesList(String[] expectedInodes, List<Inode> inodes) {
    String[] inodesName = new String[inodes.size()];
    for (int i = 0; i < inodes.size(); i++) {
      inodesName[i] = inodes.get(i).getName();
    }

    Assert.assertArrayEquals(expectedInodes, inodesName);
  }

  private static void verifyPermissionChecker(boolean enabled, String owner, String group) {
    Assert.assertEquals(enabled, Whitebox.getInternalState(PermissionChecker.class,
        "sPermissionCheckEnabled"));
    Assert.assertEquals(owner, Whitebox.getInternalState(PermissionChecker.class,
        "sFileSystemOwner"));
    Assert.assertEquals(group, Whitebox.getInternalState(PermissionChecker.class,
        "sFileSystemSuperGroup"));
  }

  @Test
  public void fileSystemOwnerTest() throws Exception {
    checkSelfPermission(TEST_USER_ADMIN, FileSystemAction.ALL, TEST_DIR_FILE_URI);
    checkSelfPermission(TEST_USER_ADMIN, FileSystemAction.ALL, TEST_DIR_URI);
    checkSelfPermission(TEST_USER_ADMIN, FileSystemAction.ALL, TEST_FILE_URI);
  }

  @Test
  public void fileSystemSuperGroupTest() throws Exception {
    checkSelfPermission(TEST_USER_SUPERGROUP, FileSystemAction.ALL, TEST_DIR_FILE_URI);
    checkSelfPermission(TEST_USER_SUPERGROUP, FileSystemAction.ALL, TEST_DIR_URI);
    checkSelfPermission(TEST_USER_SUPERGROUP, FileSystemAction.ALL, TEST_FILE_URI);
  }

  @Test
  public void selfCheckSuccessTest() throws Exception {
    // the same owner
    checkSelfPermission(TEST_USER_1, FileSystemAction.READ, TEST_DIR_FILE_URI);
    checkSelfPermission(TEST_USER_1, FileSystemAction.WRITE, TEST_DIR_FILE_URI);

    // not the owner and in other group
    checkSelfPermission(TEST_USER_2, FileSystemAction.READ, TEST_DIR_FILE_URI);

    // not the owner but in same group
    checkSelfPermission(TEST_USER_3, FileSystemAction.READ, TEST_DIR_FILE_URI);
  }

  @Test
  public void selfCheckFailByOtherGroupTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.WRITE, TEST_DIR_FILE_URI,
            "file")));

    // not the owner and in other group
    checkSelfPermission(TEST_USER_2, FileSystemAction.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void selfCheckFailBySameGroupTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_3.getUser(), FileSystemAction.WRITE, TEST_DIR_FILE_URI,
            "file")));

    // not the owner but in same group
    checkSelfPermission(TEST_USER_3, FileSystemAction.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void checkFallThroughTest() throws Exception {
    // user can not read, but group can
    checkSelfPermission(TEST_USER_1, FileSystemAction.READ, TEST_WEIRD_FILE_URI);

    // user and group can not write, but other can
    checkSelfPermission(TEST_USER_1, FileSystemAction.WRITE, TEST_WEIRD_FILE_URI);
  }

  @Test
  public void parentCheckSuccessTest() throws Exception {
    checkParentOrAncestorPermission(TEST_USER_1, FileSystemAction.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void parentCheckFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.WRITE, TEST_DIR_FILE_URI,
            "testDir")));

    checkParentOrAncestorPermission(TEST_USER_2, FileSystemAction.WRITE, TEST_DIR_FILE_URI);
  }

  @Test
  public void ancestorCheckSuccessTest() throws Exception {
    checkParentOrAncestorPermission(TEST_USER_1, FileSystemAction.WRITE, TEST_NOT_EXIST_URI);
  }

  @Test
  public void ancestorCheckFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.WRITE, TEST_NOT_EXIST_URI,
            "testDir")));

    checkParentOrAncestorPermission(TEST_USER_2, FileSystemAction.WRITE, TEST_NOT_EXIST_URI);
  }

  @Test
  public void invalidPathTest() throws Exception {
    List<FileInfo> fileInfos = Lists.newArrayList();
    mThrown.expect(InvalidPathException.class);

    PermissionChecker.checkPermission(TEST_USER_2.getUser(),
        Lists.newArrayList(TEST_USER_2.getGroups().split(",")), FileSystemAction.WRITE,
        new AlluxioURI(""), fileInfos);
  }

  private void checkSelfPermission(TestUser user, FileSystemAction action, String path)
      throws Exception {
    PermissionChecker.checkPermission(user.getUser(),
        Lists.newArrayList(user.getGroups().split(",")), action, new AlluxioURI(path),
        collectFileInfos(new AlluxioURI(path)));
  }

  private void checkParentOrAncestorPermission(TestUser user, FileSystemAction action, String path)
      throws Exception {
    PermissionChecker.checkParentPermission(user.getUser(), Lists.newArrayList(user
        .getGroups().split(",")), action, new AlluxioURI(path),
        collectFileInfos(new AlluxioURI(path)));
  }

  private List<FileInfo> collectFileInfos(AlluxioURI path) throws Exception {
    List<Inode> inodes = sTree.collectInodes(path);
    List<FileInfo> fileInfos = new ArrayList<FileInfo>();
    for (Inode inode : inodes) {
      fileInfos.add(inode.generateClientFileInfo(sTree.getPath(inode).toString()));
    }
    return fileInfos;
  }

  private String toExceptionMessage(String user, FileSystemAction action, String path,
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
