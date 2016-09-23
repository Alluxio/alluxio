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
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.security.authorization.Permission;
import alluxio.security.group.GroupMappingService;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
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
  private static final TestUser TEST_USER_SUPERGROUP = new TestUser("user4", "test-supergroup");

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

  private FileSystemMaster mFileSystemMaster;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

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

  @Before
  public void before() throws Exception {
    LoginUserTestUtils.resetLoginUser();
    GroupMappingServiceTestUtils.resetCache();
    // authentication
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    Configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "admin");
    // authorization
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        FakeUserGroupsMapping.class.getName());
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, TEST_SUPER_GROUP);

    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    Journal fsJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    BlockMaster blockMaster = new BlockMaster(blockJournal);

    mFileSystemMaster = new FileSystemMaster(blockMaster, fsJournal);

    blockMaster.start(true);
    mFileSystemMaster.start(true);

    createDirAndFileForTest();
  }

  @After
  public void after() throws Exception {
    AuthenticatedClientUser.remove();
    ConfigurationTestUtils.resetConfiguration();
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
    AuthenticatedClientUser.set(TEST_USER_ADMIN.getUser());
    mFileSystemMaster.createDirectory(new AlluxioURI("/testDir"), CreateDirectoryOptions.defaults()
        .setPermission(new Permission(TEST_USER_1.getUser(), "group1", (short) 0755)));

    // create "/testDir/file" for user1
    AuthenticatedClientUser.set(TEST_USER_1.getUser());
    mFileSystemMaster.createFile(new AlluxioURI("/testDir/file"),
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
            .setPermission(new Permission(TEST_USER_1.getUser(), "group1", (short) 0644)));

    // create "/testFile" for user2
    AuthenticatedClientUser.set(TEST_USER_ADMIN.getUser());
    mFileSystemMaster.createFile(new AlluxioURI("/testFile"),
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
            .setPermission(new Permission(TEST_USER_2.getUser(), "group2", (short) 0644)));
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
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_1.getUser(), Mode.Bits.WRITE, "/file1", "/")));
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
    AuthenticatedClientUser.set(user.getUser());
    CreateFileOptions options = CreateFileOptions.defaults().setRecursive(recursive)
        .setPermission(Permission.defaults().setOwnerFromThriftClient());

    long fileId = mFileSystemMaster.createFile(new AlluxioURI(path), options);

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    String[] pathComponents = path.split("/");
    Assert.assertEquals(pathComponents[pathComponents.length - 1], fileInfo.getName());
    Assert.assertEquals(user.getUser(), fileInfo.getOwner());
  }

  @Test
  public void mkdirUnderRootByAdmin() throws Exception {
    // createDirectory "/dir_admin" for superuser
    verifyCreateDirectory(TEST_USER_ADMIN, "/dir_admin", false);
  }

  @Test
  public void mkdirUnderRootBySupergroup() throws Exception {
    // createDirectory "/dir_admin" for superuser
    verifyCreateDirectory(TEST_USER_ADMIN, "/dir_admin", false);
  }

  @Test
  public void mkdirUnderRootByUser() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_1.getUser(), Mode.Bits.WRITE, "/dir1",
            "/")));

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
    AuthenticatedClientUser.set(user.getUser());
    CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(recursive)
        .setPermission(Permission.defaults().setOwnerFromThriftClient());
    mFileSystemMaster.createDirectory(new AlluxioURI(path), options);

    FileInfo fileInfo =
        mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(path)));
    String[] pathComponents = path.split("/");
    Assert.assertEquals(pathComponents[pathComponents.length - 1], fileInfo.getName());
    Assert.assertEquals(true, fileInfo.isFolder());
    Assert.assertEquals(user.getUser(), fileInfo.getOwner());
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
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_FILE_URI,
            "testDir")));

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
    AuthenticatedClientUser.set(user.getUser());
    String fileOwner =
        mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(srcPath)))
            .getOwner();

    mFileSystemMaster.rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath));

    Assert.assertEquals(-1, mFileSystemMaster.getFileId(new AlluxioURI(srcPath)));
    FileInfo fileInfo =
        mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(dstPath)));
    String[] pathComponents = dstPath.split("/");
    Assert.assertEquals(pathComponents[pathComponents.length - 1], fileInfo.getName());
    Assert.assertEquals(fileOwner, fileInfo.getOwner());
  }

  @Test
  public void deleteUnderRootFailed() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_1.getUser(), Mode.Bits.WRITE, TEST_DIR_URI, "/")));

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
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_URI, "/")));

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
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, TEST_DIR_FILE_URI,
            "testDir")));

    // user 2 cannot delete "/testDir/file"
    verifyDelete(TEST_USER_2, TEST_DIR_FILE_URI, false);
  }

  private void verifyDelete(TestUser user, String path, boolean recursive) throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    mFileSystemMaster.delete(new AlluxioURI(path), recursive);

    Assert.assertEquals(-1, mFileSystemMaster.getFileId(new AlluxioURI(path)));
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
    verifyGetFileInfoOrList(TEST_USER_2, dir, false);
  }

  @Test
  public void readNotExecuteDir() throws Exception {
    // set unmask
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "033");

    String dir = PathUtils.concatPath(TEST_DIR_URI, "/notExecuteDir");
    // create dir "/testDir/notExecuteDir" [user1, group1, drwxr--r--]
    verifyCreateDirectory(TEST_USER_1, dir, false);
    verifyRead(TEST_USER_1, dir, false);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.EXECUTE, dir, "notExecuteDir")));
    verifyGetFileInfoOrList(TEST_USER_2, dir, false);
  }

  private String createUnreadableFileOrDir(boolean isFile) throws Exception {
    // set unmask
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "066");

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

  private void verifyRead(TestUser user, String path, boolean isFile) throws Exception {
    verifyGetFileId(user, path);
    verifyGetFileInfoOrList(user, path, isFile);
  }

  private void verifyGetFileId(TestUser user, String path) throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    long fileId = mFileSystemMaster.getFileId(new AlluxioURI(path));
    Assert.assertNotEquals(-1, fileId);
  }

  private void verifyGetFileInfoOrList(TestUser user, String path, boolean isFile)
      throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    if (isFile) {
      Assert.assertEquals(path, mFileSystemMaster.getFileInfo(new AlluxioURI(path)).getPath());
      Assert.assertEquals(1,
          mFileSystemMaster.listStatus(new AlluxioURI(path), ListStatusOptions.defaults())
              .size());
    } else {
      List<FileInfo> fileInfoList = mFileSystemMaster
          .listStatus(new AlluxioURI(path), ListStatusOptions.defaults());
      if (fileInfoList.size() > 0) {
        Assert.assertTrue(PathUtils.getParent(fileInfoList.get(0).getPath()).equals(path));
      }
    }
  }

  @Test
  public void setStateSuccess() throws Exception {
    // set unmask
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "000");

    String file = PathUtils.concatPath(TEST_DIR_URI, "testState1");
    verifyCreateFile(TEST_USER_1, file, false);
    SetAttributeOptions expect = getNonDefaultSetState();
    SetAttributeOptions result = verifySetState(TEST_USER_2, file, expect);

    Assert.assertEquals(expect.getTtl(), result.getTtl());
    Assert.assertEquals(expect.getPinned(), result.getPinned());
  }

  @Test
  public void setStateFail() throws Exception {
    // set unmask
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "066");

    String file = PathUtils.concatPath(TEST_DIR_URI, "testState1");
    verifyCreateFile(TEST_USER_1, file, false);
    SetAttributeOptions expect = getNonDefaultSetState();

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, file, "testState1")));
    verifySetState(TEST_USER_2, file, expect);
  }

  private SetAttributeOptions getNonDefaultSetState() {
    boolean recursive = true;
    long ttl = 11;

    return SetAttributeOptions.defaults().setPinned(recursive).setTtl(ttl);
  }

  private SetAttributeOptions verifySetState(TestUser user, String path,
      SetAttributeOptions options) throws Exception {
    AuthenticatedClientUser.set(user.getUser());

    mFileSystemMaster.setAttribute(new AlluxioURI(path), options);

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(new AlluxioURI(path));
    return SetAttributeOptions.defaults().setPinned(fileInfo.isPinned()).setTtl(fileInfo.getTtl())
        .setPersisted(fileInfo.isPersisted());
  }

  @Test
  public void completeFileSuccess() throws Exception {
    // set unmask
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "044");

    String file = PathUtils.concatPath(TEST_DIR_URI, "/testState1");
    verifyCreateFile(TEST_USER_1, file, false);
    CompleteFileOptions expect = getNonDefaultCompleteFileOptions();
    verifyCompleteFile(TEST_USER_2, file, expect);
  }

  @Test
  public void completeFileFail() throws Exception {
    // set unmask
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "066");

    String file = PathUtils.concatPath(TEST_DIR_URI, "/testComplete1");
    verifyCreateFile(TEST_USER_1, file, false);
    CompleteFileOptions expect = getNonDefaultCompleteFileOptions();

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.WRITE, file, "testComplete1")));
    verifyCompleteFile(TEST_USER_2, file, expect);
  }

  private CompleteFileOptions getNonDefaultCompleteFileOptions() {
    long ufsLength = 12;
    long operationTimeMs = 21;

    return CompleteFileOptions.defaults().setUfsLength(ufsLength)
        .setOperationTimeMs(operationTimeMs);
  }

  private void verifyCompleteFile(TestUser user, String path, CompleteFileOptions options)
      throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    mFileSystemMaster.completeFile(new AlluxioURI(path), options);
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
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "066");

    String file = PathUtils.concatPath(TEST_DIR_URI, "testComplete1");
    verifyCreateFile(TEST_USER_1, file, false);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.READ, file, "testComplete1")));
    verifyFree(TEST_USER_2, file, false);
  }

  @Test
  public void freeNonNullDirectoryFail() throws Exception {
    // set unmask
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "066");

    String file = PathUtils.concatPath(TEST_DIR_URI + "/testComplete1");
    verifyCreateFile(TEST_USER_1, file, false);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), Mode.Bits.READ, file, "testComplete1")));
    verifyFree(TEST_USER_2, file, false);
  }

  private void verifyFree(TestUser user, String path, boolean recursive) throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    mFileSystemMaster.free(new AlluxioURI(path), recursive);
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
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, null, TEST_USER_1.getGroups(), (short) -1, false);

    // super group
    verifySetAcl(TEST_USER_SUPERGROUP, TEST_DIR_URI, null, TEST_USER_2.getGroups(), (short) -1,
        true);
    FileInfo fileInfo = mFileSystemMaster
        .getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroups(), fileInfo.getGroup());

    // owner
    verifySetAcl(TEST_USER_1, TEST_DIR_URI, null, TEST_USER_2.getGroups(), (short) -1, true);
    fileInfo = mFileSystemMaster
        .getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroups(), fileInfo.getGroup());
  }

  @Test
  public void setGroupFail() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        "user=" + TEST_USER_1.getUser() + " is not the owner of path=" + TEST_FILE_URI));

    verifySetAcl(TEST_USER_1, TEST_FILE_URI, null, TEST_USER_1.getGroups(), (short) -1, false);
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
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, TEST_USER_1.getUser(), TEST_USER_1.getGroups(),
        (short) 0600, false);

    // owner sets group and permission
    verifySetAcl(TEST_USER_1, TEST_DIR_URI, null, TEST_USER_2.getGroups(), (short) 0777, true);
    FileInfo fileInfo = mFileSystemMaster
        .getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroups(), fileInfo.getGroup());
    Assert.assertEquals((short) 0777, fileInfo.getMode());
  }

  @Test
  public void setAclFailByNotSuperUser() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(TEST_USER_2.getUser() + " is not a super user or in super group");

    verifySetAcl(TEST_USER_2, TEST_FILE_URI, TEST_USER_1.getUser(), TEST_USER_1.getGroups(),
        (short) 0600, false);
  }

  private void verifySetAcl(TestUser runUser, String path, String owner, String group,
      short permission, boolean recursive) throws Exception {
    AuthenticatedClientUser.set(runUser.getUser());
    SetAttributeOptions options =
        SetAttributeOptions.defaults().setOwner(owner).setGroup(group).setMode(permission)
            .setRecursive(recursive);
    mFileSystemMaster.setAttribute(new AlluxioURI(path), options);

    AuthenticatedClientUser.set(TEST_USER_ADMIN.getUser());
    FileInfo fileInfo =
        mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(new AlluxioURI(path)));
    if (owner != null) {
      Assert.assertEquals(owner, fileInfo.getOwner());
    }
    if (group != null) {
      Assert.assertEquals(group, fileInfo.getGroup());
    }
    if (permission != -1) {
      Assert.assertEquals(permission, fileInfo.getMode());
    }
  }

  private String toExceptionMessage(String user, Mode.Bits bits, String path,
      String inodeName) {
    StringBuilder stringBuilder =
        new StringBuilder().append("user=").append(user).append(", ").append("access=")
            .append(bits).append(", ").append("path=").append(path).append(": ")
            .append("failed at ").append(inodeName);
    return stringBuilder.toString();
  }
}
