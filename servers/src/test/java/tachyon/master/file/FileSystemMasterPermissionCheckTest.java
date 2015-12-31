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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.exception.AccessControlException;
import tachyon.exception.InvalidPathException;
import tachyon.master.MasterContext;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.options.CreateOptions;
import tachyon.master.file.options.MkdirOptions;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.security.authentication.PlainSaslServer;
import tachyon.security.group.GroupMappingService;
import tachyon.thrift.FileInfo;

/**
 * Unit test for {@link tachyon.master.file.FileSystemMaster} when permission check is enabled by
 * configure tachyon.security.authorization.permission.enabled=true.
 */
public class FileSystemMasterPermissionCheckTest {
  private static final String TEST_SUPER_GROUP = "test-supergroup";

  /**
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
    public void setConf(TachyonConf conf) throws IOException {
      // no-op
    }
  }

  @Before
  public void before() throws Exception {
    TachyonConf conf = new TachyonConf();
    // authentication
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");
    System.setProperty(Constants.SECURITY_LOGIN_USERNAME, "admin");
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

    // create "/testDir/file1" for user2
    verifyCreate(TEST_USER_2, TEST_DIR_URI + "/file1", false);
  }

  private void verifyCreate(TestUser user, String path, boolean recursive) throws Exception {
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    long fileId;
    if (recursive) {
      fileId = mFileSystemMaster.create(new TachyonURI(path),
          new CreateOptions.Builder(MasterContext.getConf()).setRecursive(true).build());
    } else {
      fileId = mFileSystemMaster.create(new TachyonURI(path), CreateOptions.defaults());
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
    Assert.assertEquals(true, fileInfo.isFolder);
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

    // mkdir "/testDir/dir1" for user2
    verifyMkdir(TEST_USER_2, TEST_DIR_URI + "/dir1", false);
  }

  private void verifyMkdir(TestUser user, String path, boolean recursive) throws Exception {
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    if (recursive) {
      mFileSystemMaster.mkdir(new TachyonURI(path),
          new MkdirOptions.Builder(MasterContext.getConf()).setRecursive(true).build());
    } else {
      mFileSystemMaster.mkdir(new TachyonURI(path), MkdirOptions.defaults());
    }

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new TachyonURI(path)));
    String[] pathComponents = path.split("/");
    Assert.assertEquals(pathComponents[pathComponents.length - 1], fileInfo.getName());
    Assert.assertEquals(true, fileInfo.isFolder);
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

    // rename "/testDir/file" to "/testDir/notExistDir/fileRenamed" for user1
    // This is permitted by permission checking model.
    verifyRename(TEST_USER_1, TEST_DIR_FILE_URI, "/testDir/notExistDir/fileRenamed");
  }

  @Test
  public void renameFailBySrcTest() throws Exception {
    mThrown.expect(AccessControlException.class);

    // rename "/testDir/file" to "/file" for user2
    verifyRename(TEST_USER_2, TEST_DIR_FILE_URI, "/file");
  }

  @Test
  public void renameFailByDstTest() throws Exception {
    mThrown.expect(AccessControlException.class);

    // rename "/testFile" to "/testDir/fileRenamed" for user2
    verifyRename(TEST_USER_2, TEST_FILE_URI, TEST_DIR_URI + "/fileRenamed");
  }

  private void verifyRename(TestUser user, String srcPath, String dstPath) throws Exception {
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    String fileOwner = mFileSystemMaster.getFileInfo(mFileSystemMaster.getFileId(
        new TachyonURI(srcPath))).getUserName();

    mFileSystemMaster.rename(mFileSystemMaster.getFileId(new TachyonURI(srcPath)),
        new TachyonURI(dstPath));

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

    // user2 cannot delete "/testDir" under root
    verifyDelete(TEST_USER_2, TEST_DIR_URI, true);
  }

  @Test
  public void deleteUnderRootFailOnFileTest() throws Exception {
    mThrown.expect(AccessControlException.class);

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

    // user 2 cannot delete "/testDir/file"
    verifyDelete(TEST_USER_2, TEST_DIR_FILE_URI, false);
  }

  private void verifyDelete(TestUser user, String path, boolean recursive) throws Exception {
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    mFileSystemMaster.deleteFile(mFileSystemMaster.getFileId(new TachyonURI(path)), recursive);

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
    verifyGetFileId(TEST_USER_2, file);
  }

  private void verifyGetFileId(TestUser user, String path) throws Exception {
    PlainSaslServer.AuthorizedClientUser.set(user.getUser());
    long fileId = mFileSystemMaster.getFileId(new TachyonURI(path));

    Assert.assertNotEquals(-1, fileId);
  }
}
