/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.file;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.options.SetAttributeOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.AccessControlException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidPathException;
import tachyon.master.MasterContext;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.options.CompleteFileOptions;
import tachyon.master.file.options.CreateDirectoryOptions;
import tachyon.master.file.options.CreateFileOptions;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.security.authentication.PlainSaslServer;
import tachyon.security.authorization.FileSystemAction;
import tachyon.security.group.GroupMappingService;
import tachyon.thrift.FileInfo;
import tachyon.util.io.PathUtils;

/**
 * Unit test for {@link FileSystemMaster} when permission check is enabled by configure
 * tachyon.security.authorization.permission.enabled=true.
 */
public class PermissionCheckTest {
  private static final String TEST_SUPER_GROUP = "test-supergroup";

  /**
   * The file structure for testing is:
   * <pre>{@code
   *    /               admin     admin       755
   *    /testDir        user1     group1      755
   *    /testDir/file   user1     group1      644
   *    /testFile       user2     group2      644
   * }</pre>
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
   * The user and group mappings for testing are:
   * <pre>{@code
   *    admin -> admin
   *    user1 -> group1
   *    user2 -> group2
   *    user3 -> group1
   *    user4 -> test-supergroup
   * }</pre>
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
    public void setConf(TachyonConf conf) throws IOException {
      // no-op
    }
  }

  @Before
  public void before() throws Exception {
    TachyonConf conf = new TachyonConf();
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
        new TachyonURI("/notExistDir")));
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
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    long fileId;
    if (recursive) {
      fileId = mFileSystemMaster.create(new TachyonURI(path),
          new CreateFileOptions.Builder(MasterContext.getConf()).setRecursive(true).build());
    } else {
      fileId = mFileSystemMaster.create(new TachyonURI(path), CreateFileOptions.defaults());
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
        new TachyonURI("/notExistDir")));
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
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    if (recursive) {
      mFileSystemMaster.mkdir(new TachyonURI(path),
          new CreateDirectoryOptions.Builder(MasterContext.getConf()).setRecursive(true).build());
    } else {
      mFileSystemMaster.mkdir(new TachyonURI(path), CreateDirectoryOptions.defaults());
    }

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new TachyonURI(path)));
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
    mThrown.expect(InvalidPathException.class);
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
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    String fileOwner = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new TachyonURI(srcPath))).getUserName();

    mFileSystemMaster.rename(new TachyonURI(srcPath), new TachyonURI(dstPath));

    Assert.assertEquals(-1, mFileSystemMaster.getFileId(new TachyonURI(srcPath)));
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new TachyonURI(dstPath)));
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
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    mFileSystemMaster.deleteFile(new TachyonURI(path), recursive);

    Assert.assertEquals(-1, mFileSystemMaster.getFileId(new TachyonURI(path)));
  }

  @Test
  public void getFileIdSuccessTest() throws Exception {
    verifyGetFileId(TEST_USER_1, TEST_DIR_FILE_URI);
    verifyGetFileId(TEST_USER_1, TEST_DIR_URI);
    verifyGetFileId(TEST_USER_1, TEST_FILE_URI);

    verifyGetFileId(TEST_USER_2, TEST_DIR_FILE_URI);
  }

  @Test
  public void readFileFailTest() throws Exception {
    // set unmask
    TachyonConf conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "066");
    MasterContext.reset(conf);

    // read file "/onlyReadByUser1" [user1, group1, -rw-------]
    String file = "/onlyReadByUser1";
    verifyCreate(TEST_USER_1, file, false);
    verifyGetFileId(TEST_USER_1, file);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.READ, file,
            "onlyReadByUser1")));
    verifyGetFileId(TEST_USER_2, file);
  }

  @Test
  public void readDirFailTest() throws Exception {
    // set unmask
    TachyonConf conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "066");
    MasterContext.reset(conf);

    // read dir "/testDir/testSubDir" [user1, group1, drwx--x--x]
    String file = TEST_DIR_URI + "/testSubDir";
    verifyMkdir(TEST_USER_1, file, false);
    verifyGetFileId(TEST_USER_1, file);

    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        toExceptionMessage(TEST_USER_2.getUser(), FileSystemAction.READ, file,
            "testSubDir")));
    verifyGetFileId(TEST_USER_2, file);
  }

  @Test
  public void setStateSuccessTest() throws Exception {
    // set unmask
    TachyonConf conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "044");
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
    TachyonConf conf = MasterContext.getConf();
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
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());

    mFileSystemMaster.setState(new TachyonURI(path), options);

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(new TachyonURI(path));
    return SetAttributeOptions.defaults().setPinned(fileInfo.isPinned())
        .setTtl(fileInfo.getTtl()).setPersisted(fileInfo.isPersisted());
  }

  @Test
  public void completeFileSuccessTest() throws Exception {
    // set unmask
    TachyonConf conf = MasterContext.getConf();
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
    TachyonConf conf = MasterContext.getConf();
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

    return new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(ufsLength)
        .setOperationTimeMs(operationTimeMs).build();
  }

  private void verifyCompleteFile(TestUser user, String path, CompleteFileOptions options)
      throws Exception {
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    mFileSystemMaster.completeFile(new TachyonURI(path), options);
  }

  @Test
  public void freeFileSuccessTest() throws Exception {
    // set unmask
    TachyonConf conf = MasterContext.getConf();
    conf.set(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK, "044");
    MasterContext.reset(conf);

    String file = PathUtils.concatPath(TEST_DIR_URI, "/testState1");
    verifyCreate(TEST_USER_1, file, false);
    verifyFree(TEST_USER_2, file, false);
  }

  @Test
  public void freeNonNullDirectorySuccessTest() throws Exception {
    // set unmask
    TachyonConf conf = MasterContext.getConf();
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
    TachyonConf conf = MasterContext.getConf();
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
    TachyonConf conf = MasterContext.getConf();
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
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    mFileSystemMaster.free(new TachyonURI(path), recursive);
  }

  private void verifyGetFileId(TestUser user, String path) throws Exception {
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    long fileId = mFileSystemMaster.getFileId(new TachyonURI(path));

    Assert.assertNotEquals(-1, fileId);
  }

  @Test
  public void setOwnerSuccessTest() throws Exception {
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, TEST_USER_1.getUser(), null, (short) -1, false);

    verifySetAcl(TEST_USER_SUPERGROUP, TEST_DIR_URI, TEST_USER_2.getUser(), null, (short) -1,
        true);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new TachyonURI(TEST_DIR_FILE_URI)));
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
        new TachyonURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroups(), fileInfo.getGroupName());

    // owner
    verifySetAcl(TEST_USER_1, TEST_DIR_URI, null, TEST_USER_2.getGroups(), (short) -1, true);
    fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new TachyonURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroups(), fileInfo.getGroupName());
  }

  @Test
  public void setGroupFailTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED.getMessage(
        "user=" + TEST_USER_1.getUser() + " is not the owner of path=" + TEST_FILE_URI));

    verifySetAcl(TEST_USER_1, TEST_FILE_URI, null, TEST_USER_1.getGroups(), (short) -1, false);
  }

  @Test
  public void setPermissionSuccessTest() throws Exception {
    // super user
    verifySetAcl(TEST_USER_ADMIN, TEST_FILE_URI, null, null, (short) 0600, false);

    // super group
    verifySetAcl(TEST_USER_SUPERGROUP, TEST_DIR_URI, null, null, (short) 0700, true);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new TachyonURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals((short) 0700, fileInfo.getPermission());

    // owner enlarge the permission
    verifySetAcl(TEST_USER_1, TEST_DIR_URI, null, null, (short) 0777, true);
    fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new TachyonURI(TEST_DIR_FILE_URI)));
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
        new TachyonURI(TEST_DIR_FILE_URI)));
    Assert.assertEquals(TEST_USER_2.getGroups(), fileInfo.getGroupName());
    Assert.assertEquals((short) 0777, fileInfo.getPermission());
  }

  @Test
  public void setAclFailByNotSuperuserTest() throws Exception {
    mThrown.expect(AccessControlException.class);
    mThrown.expectMessage(TEST_USER_2.getUser() + " is not a super user or in super group");

    verifySetAcl(TEST_USER_2, TEST_FILE_URI, TEST_USER_1.getUser(), TEST_USER_1.getGroups(),
        (short) 0600, false);
  }

  private void verifySetAcl(TestUser owner, String path, String user, String group,
      short permission, boolean recursive) throws Exception {
    PlainSaslServer.AuthorizedClientUser.set(owner.getUser());
    SetAttributeOptions options = SetAttributeOptions.defaults().setOwner(user).setGroup(group)
        .setPermission(permission).setRecursive(recursive);
    mFileSystemMaster.setState(new TachyonURI(path), options);

    PlainSaslServer.AuthorizedClientUser.set(TEST_USER_ADMIN.getUser());
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new TachyonURI(path)));
    if (user != null) {
      Assert.assertEquals(user, fileInfo.getUserName());
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
