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
import alluxio.AuthenticatedUserRule;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.LoginUserRule;
import alluxio.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.MasterRegistry;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectory;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MutableLockedInodePath;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.MutableJournal;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.authorization.Mode;
import alluxio.security.group.GroupMappingService;
import alluxio.util.CommonUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.TtlAction;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Unit test for {@link FileSystemMaster} when permission check is enabled by configure
 * alluxio.security.authorization.permission.enabled=true.
 */
public final class PermissionCheckTest {
  private static final String TEST_SUPER_GROUP = "test-supergroup";

  /*
   * The user and group mappings for testing are:
   *    admin -> admin
   *    user1 -> group1
   *    user2 -> group2
   *    user3 -> group1
   *    user4 -> test-supergroup
   */
  private static final TestUser TEST_USER_ADMIN = new TestUser("admin", "admin");
  private static final TestUser TEST_USER_1 = new TestUser("user1", "group1");
  private static final TestUser TEST_USER_2 = new TestUser("user2", "group2");
  private static final TestUser TEST_USER_3 = new TestUser("user3", "group1");
  private static final TestUser TEST_USER_SUPERGROUP = new TestUser("user4", TEST_SUPER_GROUP);

  /*
   * The file structure for testing is:
   *    /               admin     admin       755
   *    /testDir        user1     group1      755
   *    /testDir/file   user1     group1      644
   *    /testFile       user2     group2      644
   */
  private static final String TEST_DIR_URI = "/testDir";
  private static final String TEST_DIR_FILE_URI = "/testDir/file";
  private static final String TEST_FILE_URI = "/testFile";

  private static final Mode TEST_DIR_MODE = new Mode((short) 0755);
  private static final Mode TEST_FILE_MODE = new Mode((short) 0755);

  private MasterRegistry mRegistry;
  private FileSystemMaster mFileSystemMaster;
  private BlockMaster mBlockMaster;

  private InodeTree mInodeTree;

  @Rule
  public ConfigurationRule mConfiguration = new ConfigurationRule(ImmutableMap
      .of(PropertyKey.SECURITY_GROUP_MAPPING_CLASS, FakeUserGroupsMapping.class.getName(),
          PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, TEST_SUPER_GROUP));

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser =
      new AuthenticatedUserRule(TEST_USER_ADMIN.getUser());

  @Rule
  public LoginUserRule mLoginUserRule = new LoginUserRule(TEST_USER_ADMIN.getUser());

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

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
   * A mapping from a user to its corresponding group.
   */
  public static class FakeUserGroupsMapping implements GroupMappingService {
    private HashMap<String, String> mUserGroups = new HashMap<>();

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

  @Before
  public void before() throws Exception {
    GroupMappingServiceTestUtils.resetCache();
    mRegistry = new MasterRegistry();
    JournalFactory factory =
        new MutableJournal.Factory(new URI(mTestFolder.newFolder().getAbsolutePath()));
    mBlockMaster = new BlockMaster(mRegistry, factory);
    mFileSystemMaster = new FileSystemMaster(mRegistry, factory);
    mRegistry.start(true);

    createDirAndFileForTest();

    mInodeTree = Mockito.mock(InodeTree.class);
    Mockito.when(mInodeTree.getRootUserName()).thenReturn(TEST_USER_ADMIN.getUser());
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
    GroupMappingServiceTestUtils.resetCache();
  }

  /**
   * Sets up the following file system structure for testing.
   *    /               admin     admin       755
   *    /testDir        user1     group1      755
   *    /testDir/file   user1     group1      644
   *    /testFile       user2     group2      644
   */
  private void createDirAndFileForTest() throws Exception {
    // create "/testDir" for user1
    try (Closeable r = new AuthenticatedUserRule(TEST_USER_ADMIN.getUser()).toResource()) {
      mFileSystemMaster.createDirectory(new AlluxioURI("/testDir"),
          CreateDirectoryOptions.defaults().setOwner(TEST_USER_1.getUser())
              .setGroup(TEST_USER_1.getGroup()).setMode(TEST_DIR_MODE));
    }

    // create "/testDir/file" for user1
    try (Closeable r = new AuthenticatedUserRule(TEST_USER_1.getUser()).toResource()) {
      mFileSystemMaster.createFile(new AlluxioURI("/testDir/file"),
          CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
              .setOwner(TEST_USER_1.getUser()).setGroup(TEST_USER_1.getGroup())
              .setMode(TEST_FILE_MODE));
    }

    // create "/testFile" for user2
    try (Closeable r = new AuthenticatedUserRule(TEST_USER_ADMIN.getUser()).toResource()) {
      mFileSystemMaster.createFile(new AlluxioURI("/testFile"),
          CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
              .setOwner(TEST_USER_2.getUser()).setGroup(TEST_USER_2.getGroup())
              .setMode(TEST_FILE_MODE));
    }
  }

  private InodeDirectory getRootInode() {
    return InodeDirectory.create(0, -1, "",
        CreateDirectoryOptions.defaults().setOwner(TEST_USER_ADMIN.getUser())
            .setGroup(TEST_USER_ADMIN.getGroup()).setMode(TEST_DIR_MODE));
  }

  @Test
  public void getPermissionOwner() throws Exception {
    ArrayList<Triple<String, String, Mode>> permissions = new ArrayList<>();
    permissions.add(new ImmutableTriple<>(TEST_USER_1.getUser(), TEST_USER_1.getGroup(),
        new Mode((short) 0754)));
    LockedInodePath lockedInodePath = getLockedInodePath(permissions);
    try (Closeable r = new AuthenticatedUserRule(TEST_USER_1.getUser()).toResource()) {
      PermissionChecker checker = new PermissionChecker(mInodeTree);
      Mode.Bits actual = checker.getPermission(lockedInodePath);
      Assert.assertEquals(Mode.Bits.ALL, actual);
    }
  }

  @Test
  public void getPermissionGroup() throws Exception {
    ArrayList<Triple<String, String, Mode>> permissions = new ArrayList<>();
    permissions.add(new ImmutableTriple<>(TEST_USER_1.getUser(), TEST_USER_1.getGroup(),
        new Mode((short) 0754)));
    LockedInodePath lockedInodePath = getLockedInodePath(permissions);
    try (Closeable r = new AuthenticatedUserRule(TEST_USER_3.getUser()).toResource()) {
      PermissionChecker checker = new PermissionChecker(mInodeTree);
      Mode.Bits actual = checker.getPermission(lockedInodePath);
      Assert.assertEquals(Mode.Bits.READ_EXECUTE, actual);
    }
  }

  @Test
  public void getPermissionOther() throws Exception {
    ArrayList<Triple<String, String, Mode>> permissions = new ArrayList<>();
    permissions.add(new ImmutableTriple<>(TEST_USER_1.getUser(), TEST_USER_1.getGroup(),
        new Mode((short) 0754)));
    LockedInodePath lockedInodePath = getLockedInodePath(permissions);
    try (Closeable r  = new AuthenticatedUserRule(TEST_USER_2.getUser()).toResource()) {
      PermissionChecker checker = new PermissionChecker(mInodeTree);
      Mode.Bits actual = checker.getPermission(lockedInodePath);
      Assert.assertEquals(Mode.Bits.READ, actual);
    }
  }

  /**
   * Tests superuser and supergroup to create directories under root.
   */
  @Test
  public void createUnderRootAsAdmin() throws Exception {
    // create "/file_admin" for superuser
    verifyCreateFile(TEST_USER_ADMIN, "/file_admin", false);

    // create "/file_supergroup" for user in supergroup
    verifyCreateFile(TEST_USER_SUPERGROUP, "/file_supergroup", false);
  }

  /**
   * Tests user1 to create directories under root.
   */
  @Test
  public void createUnderRootFail() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED
        .getMessage(toExceptionMessage(TEST_USER_1.getUser(), Mode.Bits.WRITE, "/file1", "/")));
    // create "/file1" for user1
    verifyCreateFile(TEST_USER_1, "/file1", false);
  }

  @Test
  public void createSuccess() throws Exception {
    // create "/testDir/file1" for user1
    verifyCreateFile(TEST_USER_1, TEST_DIR_URI + "/file1", false);
  }

  @Test
  public void createFail() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_URI + "/file1",
            "testDir")));

    // create "/testDir/file1" for user2
    verifyCreateFile(TEST_USER_2, TEST_DIR_URI + "/file1", false);
  }

  private void verifyCreateFile(TestUser user, String path, boolean recursive) throws Exception {
    try (Closeable r = new AuthenticatedUserRule(user.getUser()).toResource()) {
      CreateFileOptions options = CreateFileOptions.defaults().setRecursive(recursive)
          .setOwner(SecurityUtils.getOwnerFromThriftClient())
          .setGroup(SecurityUtils.getGroupFromThriftClient()).setPersisted(true);

      long fileId = mFileSystemMaster.createFile(new AlluxioURI(path), options);

      FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
      String[] pathComponents = path.split("/");
      Assert.assertEquals(pathComponents[pathComponents.length - 1], fileInfo.getName());
      Assert.assertEquals(user.getUser(), fileInfo.getOwner());
    }
  }

  @Test
  public void mkdirUnderRootByAdmin() throws Exception {
    // createDirectory "/dir_admin" for superuser
    verifyCreateDirectory(TEST_USER_ADMIN, "/dir_admin", false);
  }

  @Test
  public void mkdirUnderRootBySupergroup() throws Exception {
    // createDirectory "/dir_admin" for superuser
    verifyCreateDirectory(TEST_USER_SUPERGROUP, "/dir_admin", false);
  }

  @Test
  public void mkdirUnderRootByUser() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED
        .getMessage(toExceptionMessage(TEST_USER_1.getUser(), Mode.Bits.WRITE, "/dir1", "/")));

    // createDirectory "/dir1" for user1
    verifyCreateDirectory(TEST_USER_1, "/dir1", false);
  }

  @Test
  public void mkdirSuccess() throws Exception {
    // createDirectory "/testDir/dir1" for user1
    verifyCreateDirectory(TEST_USER_1, TEST_DIR_URI + "/dir1", false);
  }

  @Test
  public void mkdirFail() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_URI + "/dir1",
            "testDir")));

    // createDirectory "/testDir/dir1" for user2
    verifyCreateDirectory(TEST_USER_2, TEST_DIR_URI + "/dir1", false);
  }

  private void verifyCreateDirectory(TestUser user, String path, boolean recursive)
      throws Exception {
    try (Closeable r = new AuthenticatedUserRule(user.getUser()).toResource()) {
      CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(recursive)
          .setOwner(SecurityUtils.getOwnerFromThriftClient())
          .setGroup(SecurityUtils.getGroupFromThriftClient());
      mFileSystemMaster.createDirectory(new AlluxioURI(path), options);

      FileInfo fileInfo =
          mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(path)));
      String[] pathComponents = path.split("/");
      Assert.assertEquals(pathComponents[pathComponents.length - 1], fileInfo.getName());
      Assert.assertEquals(true, fileInfo.isFolder());
      Assert.assertEquals(user.getUser(), fileInfo.getOwner());
    }
  }

  @Test
  public void renameUnderRootAsAdmin() throws Exception {
    // rename "/testFile" to "/testFileRenamed" for superuser
    verifyRename(TEST_USER_ADMIN, TEST_FILE_URI, "/testFileRenamed");
  }

  @Test
  public void renameUnderRootAsSupergroup() throws Exception {
    // rename "/testFile" to "/testFileRenamed" for user in supergroup
    verifyRename(TEST_USER_SUPERGROUP, TEST_FILE_URI, "/testFileRenamed");
  }

  @Test
  public void renameUnderRootFail() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_1.getUser(), Mode.Bits.WRITE, TEST_FILE_URI, "/")));

    // rename "/testFile" to "/testFileRenamed" for user1
    verifyRename(TEST_USER_1, TEST_FILE_URI, "/testFileRenamed");
  }

  @Test
  public void renameSuccess() throws Exception {
    // rename "/testDir/file" to "/testDir/fileRenamed" for user1
    verifyRename(TEST_USER_1, TEST_DIR_FILE_URI, "/testDir/fileRenamed");
  }

  @Test
  public void renameFailNotByPermission() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/testDir/notExistDir"));

    // rename "/testDir/file" to "/testDir/notExistDir/fileRenamed" for user1
    // This is permitted by permission checking model, but failed during renaming procedure,
    // since the impl cannot rename a file to a dst path whose parent does not exist.
    verifyRename(TEST_USER_1, TEST_DIR_FILE_URI, "/testDir/notExistDir/fileRenamed");
  }

  @Test
  public void renameFailBySrc() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_FILE_URI, "testDir")));

    // rename "/testDir/file" to "/file" for user2
    verifyRename(TEST_USER_2, TEST_DIR_FILE_URI, "/file");
  }

  @Test
  public void renameFailByDst() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_1.getUser(), Mode.Bits.WRITE, "/fileRenamed", "/")));

    // rename "/testDir/file" to "/fileRenamed" for user2
    verifyRename(TEST_USER_1, TEST_DIR_FILE_URI, "/fileRenamed");
  }

  private void verifyRename(TestUser user, String srcPath, String dstPath) throws Exception {
    try (Closeable r = new AuthenticatedUserRule(user.getUser()).toResource()) {
      String fileOwner =
          mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(srcPath)))
              .getOwner();

      mFileSystemMaster
          .rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath), RenameOptions.defaults());

      Assert.assertEquals(-1, mFileSystemMaster.getFileId(new AlluxioURI(srcPath)));
      FileInfo fileInfo =
          mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(dstPath)));
      String[] pathComponents = dstPath.split("/");
      Assert.assertEquals(pathComponents[pathComponents.length - 1], fileInfo.getName());
      Assert.assertEquals(fileOwner, fileInfo.getOwner());
    }
  }

  @Test
  public void deleteUnderRootFailed() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED
        .getMessage(toExceptionMessage(TEST_USER_1.getUser(), Mode.Bits.WRITE, TEST_DIR_URI, "/")));

    // delete file and dir under root by owner
    verifyDelete(TEST_USER_1, TEST_DIR_URI, true);
  }

  @Test
  public void deleteSuccessBySuperuser() throws Exception {
    // delete file and dir by superuser
    verifyDelete(TEST_USER_ADMIN, TEST_DIR_FILE_URI, false);
    verifyDelete(TEST_USER_ADMIN, TEST_DIR_URI, true);
    verifyDelete(TEST_USER_ADMIN, TEST_FILE_URI, false);
  }

  @Test
  public void deleteSuccessBySupergroup() throws Exception {
    // delete file and dir by user in supergroup
    verifyDelete(TEST_USER_SUPERGROUP, TEST_DIR_FILE_URI, false);
    verifyDelete(TEST_USER_SUPERGROUP, TEST_DIR_URI, true);
    verifyDelete(TEST_USER_SUPERGROUP, TEST_FILE_URI, false);
  }

  @Test
  public void deleteUnderRootFailOnDir() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED
        .getMessage(toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_URI, "/")));

    // user2 cannot delete "/testDir" under root
    verifyDelete(TEST_USER_2, TEST_DIR_URI, true);
  }

  @Test
  public void deleteUnderRootFailOnFile() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_1.getUser(), Mode.Bits.WRITE, TEST_FILE_URI, "/")));

    // user2 cannot delete "/testFile" under root
    verifyDelete(TEST_USER_1, TEST_FILE_URI, true);
  }

  @Test
  public void deleteSuccess() throws Exception {
    // user1 can delete its file
    verifyDelete(TEST_USER_1, TEST_DIR_FILE_URI, false);
  }

  @Test
  public void deleteFail() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_FILE_URI, "testDir")));

    // user 2 cannot delete "/testDir/file"
    verifyDelete(TEST_USER_2, TEST_DIR_FILE_URI, false);
  }

  private void verifyDelete(TestUser user, String path, boolean recursive) throws Exception {
    try (Closeable r = new AuthenticatedUserRule(user.getUser()).toResource()) {
      mFileSystemMaster.delete(new AlluxioURI(path), DeleteOptions.defaults()
          .setRecursive(recursive));
      Assert.assertEquals(-1, mFileSystemMaster.getFileId(new AlluxioURI(path)));
    }
  }

  @Test
  public void readSuccess() throws Exception {
    verifyRead(TEST_USER_1, TEST_DIR_FILE_URI, true);
    verifyRead(TEST_USER_1, TEST_DIR_URI, false);
    verifyRead(TEST_USER_1, TEST_FILE_URI, true);

    verifyRead(TEST_USER_2, TEST_DIR_FILE_URI, true);
  }

  @Test
  public void readFileIdFail() throws Exception {
    String file = createUnreadableFileOrDir(true);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.READ, file, "onlyReadByUser1")));
    verifyGetFileId(TEST_USER_2, file);
  }

  @Test
  public void readFileInfoFail() throws Exception {
    String file = createUnreadableFileOrDir(true);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.READ, file, "onlyReadByUser1")));
    verifyGetFileInfoOrList(TEST_USER_2, file, true);
  }

  @Test
  public void readDirIdFail() throws Exception {
    String dir = createUnreadableFileOrDir(false);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.READ, dir, "onlyReadByUser1")));
    verifyGetFileId(TEST_USER_2, dir);
  }

  @Test
  public void readDirInfoFail() throws Exception {
    String dir = createUnreadableFileOrDir(false);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.READ, dir, "onlyReadByUser1")));
    try (Closeable r = new AuthenticatedUserRule(TEST_USER_2.getUser()).toResource()) {
      verifyGetFileInfoOrList(TEST_USER_2, dir, false);
    }
  }

  @Test
  public void readNotExecuteDir() throws Exception {
    // set unmask
    try (Closeable c = new ConfigurationRule(
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "033").toResource()) {
      String dir = PathUtils.concatPath(TEST_DIR_URI, "/notExecuteDir");
      // create dir "/testDir/notExecuteDir" [user1, group1, drwxr--r--]
      verifyCreateDirectory(TEST_USER_1, dir, false);
      verifyRead(TEST_USER_1, dir, false);

      mThrown.expect(AccessControlException.class);
      mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
          toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.EXECUTE, dir, "notExecuteDir")));
      verifyGetFileInfoOrList(TEST_USER_2, dir, false);
    }
  }

  private String createUnreadableFileOrDir(boolean isFile) throws Exception {
    // set unmask
    try (Closeable c = new ConfigurationRule(
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "066").toResource()) {
      String fileOrDir = PathUtils.concatPath(TEST_DIR_URI, "/onlyReadByUser1");
      if (isFile) {
        // create file "/testDir/onlyReadByUser1" [user1, group1, -rw-------]
        verifyCreateFile(TEST_USER_1, fileOrDir, false);
        verifyRead(TEST_USER_1, fileOrDir, true);
      } else {
        // create dir "/testDir/onlyReadByUser1" [user1, group1, drwx--x--x]
        verifyCreateDirectory(TEST_USER_1, fileOrDir, false);
        verifyRead(TEST_USER_1, fileOrDir, false);
      }
      return fileOrDir;
    }
  }

  private void verifyRead(TestUser user, String path, boolean isFile) throws Exception {
    try (Closeable r = new AuthenticatedUserRule(user.getUser()).toResource()) {
      verifyGetFileId(user, path);
      verifyGetFileInfoOrList(user, path, isFile);
    }
  }

  private void verifyGetFileId(TestUser user, String path) throws Exception {
    try (Closeable r = new AuthenticatedUserRule(user.getUser()).toResource()) {
      long fileId = mFileSystemMaster.getFileId(new AlluxioURI(path));
      Assert.assertNotEquals(-1, fileId);
    }
  }

  private void verifyGetFileInfoOrList(TestUser user, String path, boolean isFile)
      throws Exception {
    try (Closeable r = new AuthenticatedUserRule(user.getUser()).toResource()) {
      if (isFile) {
        Assert.assertEquals(path, mFileSystemMaster.getFileInfo(new AlluxioURI(path)).getPath());
        Assert.assertEquals(1,
            mFileSystemMaster.listStatus(new AlluxioURI(path), ListStatusOptions.defaults())
                .size());
      } else {
        List<FileInfo> fileInfoList =
            mFileSystemMaster.listStatus(new AlluxioURI(path), ListStatusOptions.defaults());
        if (fileInfoList.size() > 0) {
          Assert.assertTrue(PathUtils.getParent(fileInfoList.get(0).getPath()).equals(path));
        }
      }
    }
  }

  @Test
  public void setStateSuccess() throws Exception {
    // set unmask
    try (Closeable c = new ConfigurationRule(
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "000").toResource()) {
      String file = PathUtils.concatPath(TEST_DIR_URI, "testState1");
      verifyCreateFile(TEST_USER_1, file, false);
      SetAttributeOptions expect = getNonDefaultSetState();
      SetAttributeOptions result = verifySetState(TEST_USER_2, file, expect);

      Assert.assertEquals(expect.getTtl(), result.getTtl());
      Assert.assertEquals(expect.getTtlAction(), result.getTtlAction());
      Assert.assertEquals(expect.getPinned(), result.getPinned());
    }
  }

  @Test
  public void setStateFail() throws Exception {
    // set unmask
    try (Closeable c = new ConfigurationRule(
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "066").toResource()) {
      String file = PathUtils.concatPath(TEST_DIR_URI, "testState1");
      verifyCreateFile(TEST_USER_1, file, false);
      SetAttributeOptions expect = getNonDefaultSetState();

      mThrown.expect(AccessControlException.class);
      mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
          toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, file, "testState1")));
      verifySetState(TEST_USER_2, file, expect);
    }
  }

  private SetAttributeOptions getNonDefaultSetState() {
    boolean recursive = true;
    long ttl = 11;

    return SetAttributeOptions.defaults().setPinned(recursive).setTtl(ttl)
        .setTtlAction(TtlAction.DELETE);
  }

  private SetAttributeOptions verifySetState(TestUser user, String path,
      SetAttributeOptions options) throws Exception {
    try (Closeable r = new AuthenticatedUserRule(user.getUser()).toResource()) {
      mFileSystemMaster.setAttribute(new AlluxioURI(path), options);

      FileInfo fileInfo = mFileSystemMaster.getFileInfo(new AlluxioURI(path));
      return SetAttributeOptions.defaults().setPinned(fileInfo.isPinned()).setTtl(fileInfo.getTtl())
          .setPersisted(fileInfo.isPersisted());
    }
  }

  @Test
  public void completeFileSuccess() throws Exception {
    // set unmask
    try (Closeable c = new ConfigurationRule(
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "044").toResource()) {
      String file = PathUtils.concatPath(TEST_DIR_URI, "/testState1");
      verifyCreateFile(TEST_USER_1, file, false);
      CompleteFileOptions expect = getNonDefaultCompleteFileOptions();
      verifyCompleteFile(TEST_USER_2, file, expect);
    }
  }

  @Test
  public void completeFileFail() throws Exception {
    // set unmask
    try (Closeable c = new ConfigurationRule(
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "066").toResource()) {
      String file = PathUtils.concatPath(TEST_DIR_URI, "/testComplete1");
      verifyCreateFile(TEST_USER_1, file, false);
      CompleteFileOptions expect = getNonDefaultCompleteFileOptions();

      mThrown.expect(AccessControlException.class);
      mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
          toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, file, "testComplete1")));
      verifyCompleteFile(TEST_USER_2, file, expect);
    }
  }

  private CompleteFileOptions getNonDefaultCompleteFileOptions() {
    long ufsLength = 12;
    long operationTimeMs = 21;

    return CompleteFileOptions.defaults().setUfsLength(ufsLength)
        .setOperationTimeMs(operationTimeMs);
  }

  private void verifyCompleteFile(TestUser user, String path, CompleteFileOptions options)
      throws Exception {
    try (Closeable r = new AuthenticatedUserRule(user.getUser()).toResource()) {
      mFileSystemMaster.completeFile(new AlluxioURI(path), options);
    }
  }

  @Test
  public void freeFileSuccess() throws Exception {
    String file = PathUtils.concatPath(TEST_DIR_URI, "testState1");
    verifyCreateFile(TEST_USER_1, file, false);
    verifyFree(TEST_USER_2, file, false);
  }

  @Test
  public void freeNonNullDirectorySuccess() throws Exception {
    String subDir = PathUtils.concatPath(TEST_DIR_URI, "testState");
    verifyCreateDirectory(TEST_USER_1, subDir, false);
    String file = subDir + "/testState1";
    verifyCreateFile(TEST_USER_1, file, false);
    verifyFree(TEST_USER_2, subDir, true);
  }

  @Test
  public void freeFileFail() throws Exception {
    // set unmask
    try (Closeable c = new ConfigurationRule(
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "066").toResource()) {
      String file = PathUtils.concatPath(TEST_DIR_URI, "testComplete1");
      verifyCreateFile(TEST_USER_1, file, false);

      mThrown.expect(AccessControlException.class);
      mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
          toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.READ, file, "testComplete1")));
      verifyFree(TEST_USER_2, file, false);
    }
  }

  @Test
  public void freeNonNullDirectoryFail() throws Exception {
    // set unmask
    try (Closeable c = new ConfigurationRule(
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "066").toResource()) {
      String file = PathUtils.concatPath(TEST_DIR_URI + "/testComplete1");
      verifyCreateFile(TEST_USER_1, file, false);

      mThrown.expect(AccessControlException.class);
      mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
          toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.READ, file, "testComplete1")));
      verifyFree(TEST_USER_2, file, false);
    }
  }

  private void verifyFree(TestUser user, String path, boolean recursive) throws Exception {
    try (Closeable r = new AuthenticatedUserRule(user.getUser()).toResource()) {
      mFileSystemMaster.free(new AlluxioURI(path), FreeOptions.defaults().setRecursive(recursive));
    }
  }

  @Test
  public void setOwnerSuccess() throws Exception {
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, TEST_USER_1.getUser(), null, (short) -1, false);
    verifySetAcl(TEST_USER_SUPERGROUP, TEST_DIR_URI, TEST_USER_2.getUser(), null, (short) -1, true);
    FileInfo fileInfo = mFileSystemMaster
        .getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getUser(), fileInfo.getOwner());
  }

  @Test
  public void setOwnerFail() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(TEST_USER_2.getUser() + " is not a super user or in super group");
    verifySetAcl(TEST_USER_2, TEST_FILE_URI, TEST_USER_1.getUser(), null, (short) -1, false);
  }

  @Test
  public void setGroupSuccess() throws Exception {
    // super user
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, null, TEST_USER_1.getGroup(), (short) -1, false);

    // super group
    verifySetAcl(TEST_USER_SUPERGROUP, TEST_DIR_URI, null, TEST_USER_2.getGroup(), (short) -1,
        true);
    FileInfo fileInfo = mFileSystemMaster
        .getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroup(), fileInfo.getGroup());

    // owner
    verifySetAcl(TEST_USER_1, TEST_DIR_URI, null, TEST_USER_2.getGroup(), (short) -1, true);
    fileInfo = mFileSystemMaster
        .getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroup(), fileInfo.getGroup());
  }

  @Test
  public void setGroupFail() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        "user=" + TEST_USER_1.getUser() + " is not the owner of path=" + TEST_FILE_URI));

    verifySetAcl(TEST_USER_1, TEST_FILE_URI, null, TEST_USER_1.getGroup(), (short) -1, false);
  }

  @Test
  public void setPermissionSuccess() throws Exception {
    // super user
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, null, null, (short) 0600, false);

    // super group
    verifySetAcl(TEST_USER_SUPERGROUP, TEST_DIR_URI, null, null, (short) 0700, true);
    FileInfo fileInfo = mFileSystemMaster
        .getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals((short) 0700, fileInfo.getMode());

    // owner enlarge the permission
    verifySetAcl(TEST_USER_1, TEST_DIR_URI, null, null, (short) 0777, true);
    fileInfo = mFileSystemMaster
        .getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals((short) 0777, fileInfo.getMode());
    // other user can operate under this enlarged permission
    verifyCreateFile(TEST_USER_2, TEST_DIR_URI + "/newFile", false);
    verifyDelete(TEST_USER_2, TEST_DIR_FILE_URI, false);
  }

  @Test
  public void setPermissionFail() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        "user=" + TEST_USER_1.getUser() + " is not the owner of path=" + TEST_FILE_URI));

    verifySetAcl(TEST_USER_1, TEST_FILE_URI, null, null, (short) 0777, false);
  }

  @Test
  public void setAclSuccess() throws Exception {
    // super user sets owner, group, and permission
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, TEST_USER_1.getUser(), TEST_USER_1.getGroup(),
        (short) 0600, false);

    // owner sets group and permission
    verifySetAcl(TEST_USER_1, TEST_DIR_URI, null, TEST_USER_2.getGroup(), (short) 0777, true);
    FileInfo fileInfo = mFileSystemMaster
        .getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroup(), fileInfo.getGroup());
    Assert.assertEquals((short) 0777, fileInfo.getMode());
  }

  @Test
  public void setAclFailByNotSuperUser() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(TEST_USER_2.getUser() + " is not a super user or in super group");

    verifySetAcl(TEST_USER_2, TEST_FILE_URI, TEST_USER_1.getUser(), TEST_USER_1.getGroup(),
        (short) 0600, false);
  }

  private void verifySetAcl(TestUser runUser, String path, String owner, String group,
      short mode, boolean recursive) throws Exception {
    try (Closeable r = new AuthenticatedUserRule(runUser.getUser()).toResource()) {
      SetAttributeOptions options =
          SetAttributeOptions.defaults().setOwner(owner).setGroup(group).setMode(mode)
              .setRecursive(recursive);
      mFileSystemMaster.setAttribute(new AlluxioURI(path), options);
    }
    try (Closeable r = new AuthenticatedUserRule(TEST_USER_ADMIN.getUser()).toResource()) {
      FileInfo fileInfo =
          mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(path)));
      if (owner != null) {
        Assert.assertEquals(owner, fileInfo.getOwner());
      }
      if (group != null) {
        Assert.assertEquals(group, fileInfo.getGroup());
      }
      if (mode != -1) {
        Assert.assertEquals(mode, fileInfo.getMode());
      }
    }
  }

  private String toExceptionMessage(String user, Mode.Bits bits, String path, String inodeName) {
    StringBuilder stringBuilder =
        new StringBuilder().append("user=").append(user).append(", ").append("access=").append(bits)
            .append(", ").append("path=").append(path).append(": ").append("failed at ")
            .append(inodeName);
    return stringBuilder.toString();
  }

  private LockedInodePath getLockedInodePath(ArrayList<Triple<String, String, Mode>> permissions)
      throws Exception {
    List<Inode<?>> inodes = new ArrayList<>();
    inodes.add(getRootInode());
    if (permissions.size() == 0) {
      return new MutableLockedInodePath(new AlluxioURI("/"), inodes, null);
    }
    String uri = "";
    for (int i = 0; i < permissions.size(); i++) {
      Triple<String, String, Mode> permission = permissions.get(i);
      String owner = permission.getLeft();
      String group = permission.getMiddle();
      Mode mode = permission.getRight();
      uri += "/" + (i + 1);
      if (i == permissions.size() - 1) {
        Inode<?> inode = InodeFile.create(i + 1, i, (i + 1) + "", CommonUtils.getCurrentMs(),
            CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setOwner(owner)
                .setGroup(group).setMode(mode));
        inodes.add(inode);
      } else {
        Inode<?> inode = InodeDirectory.create(i + 1, i, (i + 1) + "",
            CreateDirectoryOptions.defaults().setOwner(owner).setGroup(group).setMode(mode));
        inodes.add(inode);
      }
    }
    return new MutableLockedInodePath(new AlluxioURI(uri), inodes, null);
  }
}
