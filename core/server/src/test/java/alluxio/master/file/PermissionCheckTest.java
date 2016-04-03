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
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.MasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.FileSystemAction;
import alluxio.security.group.GroupMappingService;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Unit test for {@link FileSystemMaster} when permission check is enabled by configure
 * alluxio.security.authorization.permission.enabled=true.
 */
public class PermissionCheckTest {
  private static final String TEST_SUPER_GROUP = "test-supergroup";

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
  private static final TestUser TEST_USER_SUPERGROUP =
      new TestUser("user4", "test-supergroup");

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
    private HashMap<String, String> mUserGroups = new HashMap<String, String>();

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
      return Lists.newArrayList();
    }

    @Override
    public void setConf(Configuration conf) throws IOException {
      // no-op
    }
  }

  @Before
  public void before() throws Exception {
    Configuration conf = new Configuration();
    // authentication
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");
    conf.set(Constants.SECURITY_LOGIN_USERNAME, "admin");
    // authorization
    conf.set(Constants.SECURITY_GROUP_MAPPING, FakeUserGroupsMapping.class.getName());
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, TEST_SUPER_GROUP);
    MasterContext.reset(conf);

    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    Journal fsJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    BlockMaster blockMaster = new BlockMaster(blockJournal);

    mFileSystemMaster = new FileSystemMaster(blockMaster, fsJournal);

    blockMaster.start(true);
    mFileSystemMaster.start(true);

    createDirAndFileForTest();
  }

  private void createDirAndFileForTest() throws Exception {
    // create "/testDir" for user1
    verifyMkdir(TEST_USER_1, TEST_DIR_URI, false);

    // create "/testDir/file" for user1
    verifyCreate(TEST_USER_1, TEST_DIR_FILE_URI, false);

    // create "/testDir/file" for user2
    verifyCreate(TEST_USER_2, TEST_FILE_URI, false);
  }

  @Test
  public void createUnderRootTest() throws Exception {
    // create "/file1" for user1
    verifyCreate(TEST_USER_1, "/file1", false);

    // create "/file_admin" for superuser
    verifyCreate(TEST_USER_ADMIN, "/file_admin", false);

    // create "/file_supergroup" for user in supergroup
    verifyCreate(TEST_USER_SUPERGROUP, "/file_supergroup", false);

    // create nested "/notExistDir/notExistFile" for user1
    verifyCreate(TEST_USER_1, "/notExistDir/notExistFile", true);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI("/notExistDir")));
    Assert.assertEquals("notExistDir", fileInfo.getName());
    Assert.assertEquals(TEST_USER_1.getUser(), fileInfo.getUserName());
  }

  @Test
  public void createSuccessTest() throws Exception {
    // create "/testDir/file1" for user1
    verifyCreate(TEST_USER_1, TEST_DIR_URI + "/file1", false);
  }

  @Test
  public void createFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.WRITE, TEST_DIR_URI + "/file1",
            "testDir")));

    // create "/testDir/file1" for user2
    verifyCreate(TEST_USER_2, TEST_DIR_URI + "/file1", false);
  }

  private void verifyCreate(TestUser user, String path, boolean recursive) throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    long fileId;
    if (recursive) {
      fileId = mFileSystemMaster
          .create(new AlluxioURI(path), CreateFileOptions.defaults().setRecursive(true));
    } else {
      fileId = mFileSystemMaster.create(new AlluxioURI(path), CreateFileOptions.defaults());
    }

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    String[] pathComponents = path.split("/");
    Assert.assertEquals(pathComponents[pathComponents.length - 1], fileInfo.getName());
    Assert.assertEquals(user.getUser(), fileInfo.getUserName());
  }

  @Test
  public void mkdirUnderRootTest() throws Exception {
    // mkdir "/dir1" for user1
    verifyMkdir(TEST_USER_1, "/dir1", false);

    // mkdir "/dir_admin" for superuser
    verifyMkdir(TEST_USER_ADMIN, "/dir_admin", false);

    // mkdir "/dir_supergroup" for user in supergroup
    verifyMkdir(TEST_USER_SUPERGROUP, "/dir_supergroup", false);

    // mkdir nested "/notExistDir/notExistSubDir" for user1
    verifyMkdir(TEST_USER_1, "/notExistDir/notExistSubDir", true);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI("/notExistDir")));
    Assert.assertEquals("notExistDir", fileInfo.getName());
    Assert.assertEquals(true, fileInfo.isFolder());
    Assert.assertEquals(TEST_USER_1.getUser(), fileInfo.getUserName());
  }

  @Test
  public void mkdirSuccessTest() throws Exception {
    // mkdir "/testDir/dir1" for user1
    verifyMkdir(TEST_USER_1, TEST_DIR_URI + "/dir1", false);
  }

  @Test
  public void mkdirFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.WRITE, TEST_DIR_URI + "/dir1",
            "testDir")));

    // mkdir "/testDir/dir1" for user2
    verifyMkdir(TEST_USER_2, TEST_DIR_URI + "/dir1", false);
  }

  private void verifyMkdir(TestUser user, String path, boolean recursive) throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    if (recursive) {
      mFileSystemMaster
          .mkdir(new AlluxioURI(path), CreateDirectoryOptions.defaults().setRecursive(true));
    } else {
      mFileSystemMaster.mkdir(new AlluxioURI(path), CreateDirectoryOptions.defaults());
    }

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI(path)));
    String[] pathComponents = path.split("/");
    Assert.assertEquals(pathComponents[pathComponents.length - 1], fileInfo.getName());
    Assert.assertEquals(true, fileInfo.isFolder());
    Assert.assertEquals(user.getUser(), fileInfo.getUserName());
  }

  @Test
  public void renameUnderRootSuccessTest() throws Exception {
    // rename "/testFile" to "/testFileRenamed" for user2
    verifyRename(TEST_USER_2, TEST_FILE_URI, "/testFileRenamed");

    // rename "/testFile" to "/testFileRenamed" for superuser
    verifyRename(TEST_USER_ADMIN, "/testFileRenamed", TEST_FILE_URI);

    // rename "/testFile" to "/testFileRenamed" for user in supergroup
    verifyRename(TEST_USER_SUPERGROUP, TEST_FILE_URI, "/testFileRenamed");
  }

  @Test
  public void renameUnderRootFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        "user=" + TEST_USER_1.getUser() + " is not the owner of path=" + TEST_FILE_URI));

    // rename "/testFile" to "/testFileRenamed" for user1
    verifyRename(TEST_USER_1, TEST_FILE_URI, "/testFileRenamed");
  }

  @Test
  public void renameSuccessTest() throws Exception {
    // rename "/testDir/file" to "/testDir/fileRenamed" for user1
    verifyRename(TEST_USER_1, TEST_DIR_FILE_URI, "/testDir/fileRenamed");
  }

  @Test
  public void renameFailNotByPermissionTest() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/testDir/notExistDir"));

    // rename "/testDir/file" to "/testDir/notExistDir/fileRenamed" for user1
    // This is permitted by permission checking model, but failed during renaming procedure,
    // since the impl cannot rename a file to a dst path whose parent does not exist.
    verifyRename(TEST_USER_1, TEST_DIR_FILE_URI, "/testDir/notExistDir/fileRenamed");
  }

  @Test
  public void renameFailBySrcTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.WRITE, TEST_DIR_FILE_URI,
            "testDir")));

    // rename "/testDir/file" to "/file" for user2
    verifyRename(TEST_USER_2, TEST_DIR_FILE_URI, "/file");
  }

  @Test
  public void renameFailByDstTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.WRITE,
            TEST_DIR_URI + "/fileRenamed", "testDir")));

    // rename "/testFile" to "/testDir/fileRenamed" for user2
    verifyRename(TEST_USER_2, TEST_FILE_URI, TEST_DIR_URI + "/fileRenamed");
  }

  private void verifyRename(TestUser user, String srcPath, String dstPath) throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    String fileOwner = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI(srcPath))).getUserName();

    mFileSystemMaster.rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath));

    Assert.assertEquals(-1, mFileSystemMaster.getFileId(new AlluxioURI(srcPath)));
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI(dstPath)));
    String[] pathComponents = dstPath.split("/");
    Assert.assertEquals(pathComponents[pathComponents.length - 1], fileInfo.getName());
    Assert.assertEquals(fileOwner, fileInfo.getUserName());
  }

  @Test
  public void deleteUnderRootSuccessTest() throws Exception {
    // delete file and dir under root by owner
    verifyDelete(TEST_USER_1, TEST_DIR_URI, true);
    verifyDelete(TEST_USER_2, TEST_FILE_URI, false);
  }

  @Test
  public void deleteSuccessBySuperuserTest() throws Exception {
    // delete file and dir by superuser
    verifyDelete(TEST_USER_ADMIN, TEST_DIR_FILE_URI, false);
    verifyDelete(TEST_USER_ADMIN, TEST_DIR_URI, true);
    verifyDelete(TEST_USER_ADMIN, TEST_FILE_URI, false);
  }

  @Test
  public void deleteSuccessBySupergroupTest() throws Exception {
    // delete file and dir by user in supergroup
    verifyDelete(TEST_USER_SUPERGROUP, TEST_DIR_FILE_URI, false);
    verifyDelete(TEST_USER_SUPERGROUP, TEST_DIR_URI, true);
    verifyDelete(TEST_USER_SUPERGROUP, TEST_FILE_URI, false);
  }

  @Test
  public void deleteUnderRootFailOnDirTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        "user=" + TEST_USER_2.getUser() + " is not the owner of path=" + TEST_DIR_URI));

    // user2 cannot delete "/testDir" under root
    verifyDelete(TEST_USER_2, TEST_DIR_URI, true);
  }

  @Test
  public void deleteUnderRootFailOnFileTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        "user=" + TEST_USER_1.getUser() + " is not the owner of path=" + TEST_FILE_URI));

    // user2 cannot delete "/testFile" under root
    verifyDelete(TEST_USER_1, TEST_FILE_URI, true);
  }

  @Test
  public void deleteSuccessTest() throws Exception {
    // user1 can delete its file
    verifyDelete(TEST_USER_1, TEST_DIR_FILE_URI, false);
  }

  @Test
  public void deleteFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.WRITE, TEST_DIR_FILE_URI,
            "testDir")));

    // user 2 cannot delete "/testDir/file"
    verifyDelete(TEST_USER_2, TEST_DIR_FILE_URI, false);
  }

  private void verifyDelete(TestUser user, String path, boolean recursive) throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    mFileSystemMaster.deleteFile(new AlluxioURI(path), recursive);

    Assert.assertEquals(-1, mFileSystemMaster.getFileId(new AlluxioURI(path)));
  }

  @Test
  public void readSuccessTest() throws Exception {
    verifyRead(TEST_USER_1, TEST_DIR_FILE_URI, true);
    verifyRead(TEST_USER_1, TEST_DIR_URI, false);
    verifyRead(TEST_USER_1, TEST_FILE_URI, true);

    verifyRead(TEST_USER_2, TEST_DIR_FILE_URI, true);
  }

  @Test
  public void readFileIdFailTest() throws Exception {
    String file = createUnreadableFileOrDir(true);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.READ, file, "onlyReadByUser1")));
    verifyGetFileId(TEST_USER_2, file);
  }

  @Test
  public void readFileInfoFailTest() throws Exception {
    String file = createUnreadableFileOrDir(true);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.READ, file, "onlyReadByUser1")));
    verifyGetFileInfoOrList(TEST_USER_2, file, true);
  }

  @Test
  public void readDirIdFailTest() throws Exception {
    String dir = createUnreadableFileOrDir(false);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.READ, dir, "onlyReadByUser1")));
    verifyGetFileId(TEST_USER_2, dir);
  }

  @Test
  public void readDirInfoFailTest() throws Exception {
    String dir = createUnreadableFileOrDir(false);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.READ, dir, "onlyReadByUser1")));
    verifyGetFileInfoOrList(TEST_USER_2, dir, false);
  }

  @Test
  public void readNotExecuteDirTest() throws Exception {
    // set unmask
    Configuration conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "033");
    MasterContext.reset(conf);

    String dir = PathUtils.concatPath(TEST_DIR_URI, "/notExecuteDir");
    // create dir "/testDir/notExecuteDir" [user1, group1, drwxr--r--]
    verifyMkdir(TEST_USER_1, dir, false);
    verifyRead(TEST_USER_1, dir, false);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.EXECUTE, dir,
            "notExecuteDir")));
    verifyGetFileInfoOrList(TEST_USER_2, dir, false);
  }

  private String createUnreadableFileOrDir(boolean isFile) throws Exception {
    // set unmask
    Configuration conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "066");
    MasterContext.reset(conf);

    String fileOrDir = PathUtils.concatPath(TEST_DIR_URI, "/onlyReadByUser1");
    if (isFile) {
      // create file "/testDir/onlyReadByUser1" [user1, group1, -rw-------]
      verifyCreate(TEST_USER_1, fileOrDir, false);
      verifyRead(TEST_USER_1, fileOrDir, true);
    } else {
      // create dir "/testDir/onlyReadByUser1" [user1, group1, drwx--x--x]
      verifyMkdir(TEST_USER_1, fileOrDir, false);
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

  private void verifyGetFileInfoOrList(TestUser user, String path,
      boolean isFile) throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    if (isFile) {
      Assert.assertEquals(path, mFileSystemMaster.getFileInfo(new AlluxioURI(path)).getPath());
      Assert.assertEquals(1, mFileSystemMaster.getFileInfoList(new AlluxioURI(path)).size());
    } else {
      List<FileInfo> fileInfoList = mFileSystemMaster.getFileInfoList(new AlluxioURI(path));
      if (fileInfoList.size() > 0) {
        Assert.assertTrue(PathUtils.getParent(fileInfoList.get(0).getPath()).equals(path));
      }
    }
  }

  @Test
  public void setStateSuccessTest() throws Exception {
    // set unmask
    Configuration conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "000");
    MasterContext.reset(conf);

    String file = PathUtils.concatPath(TEST_DIR_URI, "testState1");
    verifyCreate(TEST_USER_1, file, false);
    SetAttributeOptions expect = getNonDefaultSetState();
    SetAttributeOptions result = verifySetState(TEST_USER_2, file, expect);

    Assert.assertEquals(expect.getTtl(), result.getTtl());
    Assert.assertEquals(expect.getPinned(), result.getPinned());
  }

  @Test
  public void setStateFailTest() throws Exception {
    // set unmask
    Configuration conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "066");
    MasterContext.reset(conf);

    String file = PathUtils.concatPath(TEST_DIR_URI, "testState1");
    verifyCreate(TEST_USER_1, file, false);
    SetAttributeOptions expect = getNonDefaultSetState();

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(toExceptionMessage(
        TEST_USER_2.getUser(), FileSystemAction.WRITE, file, "testState1")));
    verifySetState(TEST_USER_2, file, expect);
  }

  private SetAttributeOptions getNonDefaultSetState() {
    boolean recursive = true;
    long ttl = 11;

    return SetAttributeOptions.defaults().setPinned(recursive).setTtl(ttl);
  }

  private SetAttributeOptions verifySetState(
      TestUser user, String path, SetAttributeOptions options) throws Exception {
    AuthenticatedClientUser.set(user.getUser());

    mFileSystemMaster.setAttribute(new AlluxioURI(path), options);

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(new AlluxioURI(path));
    return SetAttributeOptions.defaults().setPinned(fileInfo.isPinned()).setTtl(fileInfo.getTtl())
        .setPersisted(fileInfo.isPersisted());
  }

  @Test
  public void completeFileSuccessTest() throws Exception {
    // set unmask
    Configuration conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "044");
    MasterContext.reset(conf);

    String file = PathUtils.concatPath(TEST_DIR_URI, "/testState1");
    verifyCreate(TEST_USER_1, file, false);
    CompleteFileOptions expect = getNonDefaultCompleteFileOptions();
    verifyCompleteFile(TEST_USER_2, file, expect);
  }

  @Test
  public void completeFileFailTest() throws Exception {
    // set unmask
    Configuration conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "066");
    MasterContext.reset(conf);

    String file = PathUtils.concatPath(TEST_DIR_URI, "/testComplete1");
    verifyCreate(TEST_USER_1, file, false);
    CompleteFileOptions expect = getNonDefaultCompleteFileOptions();

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(toExceptionMessage(
        TEST_USER_2.getUser(), FileSystemAction.WRITE, file, "testComplete1")));
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
  public void freeFileSuccessTest() throws Exception {
    // set unmask
    Configuration conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "044");
    MasterContext.reset(conf);

    String file = PathUtils.concatPath(TEST_DIR_URI, "/testState1");
    verifyCreate(TEST_USER_1, file, false);
    verifyFree(TEST_USER_2, file, false);
  }

  @Test
  public void freeNonNullDirectorySuccessTest() throws Exception {
    // set unmask
    Configuration conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "044");
    MasterContext.reset(conf);

    String subDir = PathUtils.concatPath(TEST_DIR_URI, "/testState");
    verifyMkdir(TEST_USER_1, subDir, false);
    String file = subDir + "/testState1";
    verifyCreate(TEST_USER_1, file, false);
    verifyFree(TEST_USER_2, subDir, true);
  }

  @Test
  public void freeFileFailTest() throws Exception {
    // set unmask
    Configuration conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "066");
    MasterContext.reset(conf);

    String file = PathUtils.concatPath(TEST_DIR_URI, "/testComplete1");
    verifyCreate(TEST_USER_1, file, false);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(toExceptionMessage(
        TEST_USER_2.getUser(), FileSystemAction.WRITE, file, "testComplete1")));
    verifyFree(TEST_USER_2, file, false);
  }

  @Test
  public void freeNonNullDirectoryFailTest() throws Exception {
    // set unmask
    Configuration conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "066");
    MasterContext.reset(conf);

    String file = PathUtils.concatPath(TEST_DIR_URI + "/testComplete1");
    verifyCreate(TEST_USER_1, file, false);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(toExceptionMessage(
        TEST_USER_2.getUser(), FileSystemAction.WRITE, file, "testComplete1")));
    verifyFree(TEST_USER_2, file, false);
  }

  private void verifyFree(TestUser user, String path, boolean recursive) throws Exception {
    AuthenticatedClientUser.set(user.getUser());
    mFileSystemMaster.free(new AlluxioURI(path), recursive);
  }

  @Test
  public void setOwnerSuccessTest() throws Exception {
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, TEST_USER_1.getUser(), null, (short) -1, false);
    verifySetAcl(TEST_USER_SUPERGROUP, TEST_DIR_URI, TEST_USER_2.getUser(), null, (short) -1, true);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getUser(), fileInfo.getUserName());
  }

  @Test
  public void setOwnerFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(TEST_USER_2.getUser() + " is not a super user or in super group");
    verifySetAcl(TEST_USER_2, TEST_FILE_URI, TEST_USER_1.getUser(), null, (short) -1, false);
  }

  @Test
  public void setGroupSuccessTest() throws Exception {
    // super user
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, null, TEST_USER_1.getGroups(), (short) -1, false);

    // super group
    verifySetAcl(TEST_USER_SUPERGROUP, TEST_DIR_URI, null, TEST_USER_2.getGroups(), (short) -1,
        true);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroups(), fileInfo.getGroupName());

    // owner
    verifySetAcl(TEST_USER_1, TEST_DIR_URI, null, TEST_USER_2.getGroups(), (short) -1, true);
    fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroups(), fileInfo.getGroupName());
  }

  @Test
  public void setGroupFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage("user="
        + TEST_USER_1.getUser() + " is not the owner of path=" + TEST_FILE_URI));

    verifySetAcl(TEST_USER_1, TEST_FILE_URI, null, TEST_USER_1.getGroups(), (short) -1, false);
  }

  @Test
  public void setPermissionSuccessTest() throws Exception {
    // super user
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, null, null, (short) 0600, false);

    // super group
    verifySetAcl(TEST_USER_SUPERGROUP, TEST_DIR_URI, null, null, (short) 0700, true);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals((short) 0700, fileInfo.getPermission());

    // owner enlarge the permission
    verifySetAcl(TEST_USER_1, TEST_DIR_URI, null, null, (short) 0777, true);
    fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals((short) 0777, fileInfo.getPermission());
    // other user can operate under this enlarged permission
    verifyCreate(TEST_USER_2, TEST_DIR_URI + "/newFile", false);
    verifyDelete(TEST_USER_2, TEST_DIR_FILE_URI, false);
  }

  @Test
  public void setPermissionFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        "user=" + TEST_USER_1.getUser() + " is not the owner of path=" + TEST_FILE_URI));

    verifySetAcl(TEST_USER_1, TEST_FILE_URI, null, null, (short) 0777, false);
  }

  @Test
  public void setAclSuccessTest() throws Exception {
    // super user sets owner, group, and permission
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, TEST_USER_1.getUser(), TEST_USER_1.getGroups(),
        (short) 0600, false);

    // owner sets group and permission
    verifySetAcl(TEST_USER_1, TEST_DIR_URI, null, TEST_USER_2.getGroups(), (short) 0777, true);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroups(), fileInfo.getGroupName());
    Assert.assertEquals((short) 0777, fileInfo.getPermission());
  }

  @Test
  public void setAclFailByNotSuperUserTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(TEST_USER_2.getUser() + " is not a super user or in super group");

    verifySetAcl(TEST_USER_2, TEST_FILE_URI, TEST_USER_1.getUser(), TEST_USER_1.getGroups(),
        (short) 0600, false);
  }

  private void verifySetAcl(TestUser runUser, String path, String owner, String group,
      short permission, boolean recursive) throws Exception {
    AuthenticatedClientUser.set(runUser.getUser());
    SetAttributeOptions options =
        SetAttributeOptions.defaults().setOwner(owner).setGroup(group).setPermission(permission)
            .setRecursive(recursive);
    mFileSystemMaster.setAttribute(new AlluxioURI(path), options);

    AuthenticatedClientUser.set(TEST_USER_ADMIN.getUser());
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new AlluxioURI(path)));
    if (owner != null) {
      Assert.assertEquals(owner, fileInfo.getUserName());
    }
    if (group != null) {
      Assert.assertEquals(group, fileInfo.getGroupName());
    }
    if (permission != -1) {
      Assert.assertEquals(permission, fileInfo.getPermission());
    }
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
