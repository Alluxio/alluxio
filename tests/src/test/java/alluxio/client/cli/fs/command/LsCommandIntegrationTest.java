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
import alluxio.PropertyKey;
import alluxio.cli.fs.command.LsCommand;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.SetAclOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AlluxioException;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.master.file.meta.PersistenceState;
import alluxio.security.authorization.AclEntry;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.SetAclAction;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Tests for ls command.
 */
public final class LsCommandIntegrationTest extends AbstractFileSystemShellTest {
  private static final String STATE_FILE_IN_ALLUXIO = "100%";
  private static final String STATE_FILE_NOT_IN_ALLUXIO = "0%";

  // Helper function to format ls result.
  private String getLsResultStr(AlluxioURI uri, int size, String testUser, String testGroup)
      throws IOException, AlluxioException {
    URIStatus status = mFileSystem.getStatus(uri);
    // detect the extended acls
    boolean hasExtended = status.getAcl().hasExtended()
        || !status.getDefaultAcl().isEmpty();

    return getLsResultStr(uri.getPath(), status.getLastModificationTimeMs(), size,
        STATE_FILE_IN_ALLUXIO, testUser, testGroup, status.getMode(), hasExtended,
        status.isFolder(), PersistenceState.NOT_PERSISTED.name());
  }

  // Helper function to format ls result.
  private String getLsResultStr(String path, long createTime, int size, String inAlluxioState,
      String testUser, String testGroup, int permission, boolean hasExtended, boolean isDir,
      String persistenceState)
      throws IOException, AlluxioException {
    return String
        .format(LsCommand.LS_FORMAT, FormatUtils.formatMode((short) permission, isDir, hasExtended),
            testUser, testGroup, String.valueOf(size), persistenceState,
            CommonUtils.convertMsToDate(createTime),
            isDir ? LsCommand.IN_ALLUXIO_STATE_DIR : inAlluxioState, path);
  }

  // Helper function to format ls result without acl enabled.
  private String getLsNoAclResultStr(AlluxioURI uri, int size, String inAlluxioState,
      String persistenceState) throws IOException, AlluxioException {
    URIStatus status = mFileSystem.getStatus(uri);
    return getLsNoAclResultStr(uri.getPath(), status.getLastModificationTimeMs(), size,
        inAlluxioState, persistenceState);
  }

  // Helper function to format ls result without acl enabled.
  private String getLsNoAclResultStr(String path, long createTime, int size, String inAlluxioState,
      String persistenceState) throws IOException, AlluxioException {
    return getLsNoAclResultStr(path, createTime, false, size, inAlluxioState,
        persistenceState);
  }

  // Helper function to format ls result without acl enabled.
  private String getLsNoAclResultStr(String path, long createTime, boolean hSize, int size,
      String inAlluxioState, String persistenceState)
      throws IOException, AlluxioException {
    String sizeStr;
    if (inAlluxioState.equals(LsCommand.IN_ALLUXIO_STATE_DIR)) {
      sizeStr = String.valueOf(size);
    } else {
      sizeStr = hSize ? FormatUtils.getSizeFromBytes(size) : String.valueOf(size);
    }
    return String.format(LsCommand.LS_FORMAT_NO_ACL, sizeStr, persistenceState,
        CommonUtils.convertMsToDate(createTime), inAlluxioState, path);
  }

  // Helper function to create a set of files in the file system
  private URIStatus[] createFiles() throws IOException, AlluxioException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testDir/testFileB", WriteType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testFileC", WriteType.THROUGH, 30);

    URIStatus[] files = new URIStatus[4];
    files[0] = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileA"));
    files[1] = mFileSystem.getStatus(new AlluxioURI("/testRoot/testDir"));
    files[2] = mFileSystem.getStatus(new AlluxioURI("/testRoot/testDir/testFileB"));
    files[3] = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileC"));
    return files;
  }

  /**
   * Tests ls command when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsNoAcl() throws IOException, AlluxioException {
    URIStatus[] files = createFiles();
    mFsShell.run("ls", "/testRoot");
    String expected = "";
    expected += getLsNoAclResultStr("/testRoot/testDir", files[1].getLastModificationTimeMs(), 1,
        LsCommand.IN_ALLUXIO_STATE_DIR, files[1].getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileA", files[0].getLastModificationTimeMs(), 10,
        STATE_FILE_IN_ALLUXIO, files[0].getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileC", files[3].getLastModificationTimeMs(), 30,
        STATE_FILE_NOT_IN_ALLUXIO, files[3].getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls -h command when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsHumanReadable() throws IOException, AlluxioException {
    URIStatus[] files = createFiles();
    mFsShell.run("ls", "-h", "/testRoot");
    boolean hSize = true;
    String expected = "";
    expected += getLsNoAclResultStr("/testRoot/testDir", files[1].getLastModificationTimeMs(),
        hSize, 1, LsCommand.IN_ALLUXIO_STATE_DIR, files[1].getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileA", files[0].getLastModificationTimeMs(),
        hSize, 10, STATE_FILE_IN_ALLUXIO, files[0].getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileC", files[3].getLastModificationTimeMs(),
        hSize, 30, STATE_FILE_NOT_IN_ALLUXIO, files[3].getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls -p command when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsPinned() throws IOException, AlluxioException {
    URIStatus[] files = createFiles();
    AlluxioURI fileURI1 = new AlluxioURI("/testRoot/testDir/testFileB");
    AlluxioURI fileURI2 = new AlluxioURI("/testRoot/testFileA");
    mFileSystem.setAttribute(fileURI1,
        SetAttributeOptions.defaults().setPinned(true));
    mFileSystem.setAttribute(fileURI2,
        SetAttributeOptions.defaults().setPinned(true));
    URIStatus file1 = mFileSystem.getStatus(fileURI1);
    URIStatus file2 = mFileSystem.getStatus(fileURI2);
    mFsShell.run("ls", "-pR",  "/testRoot");
    String expected = "";
    expected += getLsNoAclResultStr(fileURI1.toString(), file1.getLastModificationTimeMs(), 20,
        STATE_FILE_IN_ALLUXIO, file1.getPersistenceState());
    expected += getLsNoAclResultStr(fileURI2.toString(), file2.getLastModificationTimeMs(), 10,
        STATE_FILE_IN_ALLUXIO, file2.getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls -d command when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsDirectoryAsPlainFileNoAcl() throws IOException, AlluxioException {
    URIStatus[] files = createFiles();
    mFsShell.run("ls", "-d", "/testRoot");
    URIStatus dirStatus = mFileSystem.getStatus(new AlluxioURI("/testRoot/"));
    String expected = "";
    expected += getLsNoAclResultStr("/testRoot", dirStatus.getLastModificationTimeMs(),
        3 /* number of direct children under /testRoot/ dir */, LsCommand.IN_ALLUXIO_STATE_DIR,
        dirStatus.getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls -d command on root directory when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsRootNoAcl() throws IOException, AlluxioException {
    mFsShell.run("ls", "-d", "/");
    URIStatus dirStatus = mFileSystem.getStatus(new AlluxioURI("/"));
    String expected = "";
    expected +=
        getLsNoAclResultStr("/", dirStatus.getLastModificationTimeMs(), 0,
            LsCommand.IN_ALLUXIO_STATE_DIR, dirStatus.getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls command when security is enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
          PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS,
          "alluxio.security.group.provider.IdentityUserGroupsMapping",
          PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test_user_ls"})
  public void ls() throws Exception {
    String testUser = "test_user_ls";
    clearAndLogin(testUser);
    URIStatus[] files = createFiles();
    mFsShell.run("ls", "/testRoot");
    String expected = "";
    expected +=
        getLsResultStr("/testRoot/testDir", files[1].getLastModificationTimeMs(), 1,
            LsCommand.IN_ALLUXIO_STATE_DIR, testUser, testUser, files[1].getMode(), false,
            files[1].isFolder(), files[1].getPersistenceState());
    expected += getLsResultStr("/testRoot/testFileA", files[0].getLastModificationTimeMs(), 10,
        STATE_FILE_IN_ALLUXIO, testUser, testUser, files[0].getMode(), false,
        files[0].isFolder(), files[0].getPersistenceState());
    expected += getLsResultStr("/testRoot/testFileC", files[3].getLastModificationTimeMs(), 30,
        STATE_FILE_NOT_IN_ALLUXIO, testUser, testUser, files[3].getMode(), false,
        files[3].isFolder(), files[3].getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls command with wildcard when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsWildcardNoAcl() throws IOException, AlluxioException {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);

    String expect = "";
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/bar/foobar3"), 30,
        STATE_FILE_IN_ALLUXIO, PersistenceState.NOT_PERSISTED.name());
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/foo/foobar1"), 10,
        STATE_FILE_IN_ALLUXIO, PersistenceState.NOT_PERSISTED.name());
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/foo/foobar2"), 20,
        STATE_FILE_IN_ALLUXIO, PersistenceState.NOT_PERSISTED.name());
    mFsShell.run("ls", testDir + "/*/foo*");
    Assert.assertEquals(expect, mOutput.toString());

    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/bar/foobar3"), 30,
        STATE_FILE_IN_ALLUXIO, PersistenceState.NOT_PERSISTED.name());
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/foo/foobar1"), 10,
        STATE_FILE_IN_ALLUXIO, PersistenceState.NOT_PERSISTED.name());
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/foo/foobar2"), 20,
        STATE_FILE_IN_ALLUXIO, PersistenceState.NOT_PERSISTED.name());
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/foobar4"), 40, STATE_FILE_IN_ALLUXIO,
        PersistenceState.NOT_PERSISTED.name());
    mFsShell.run("ls", testDir + "/*");
    Assert.assertEquals(expect, mOutput.toString());
  }

  /**
   * Tests ls command with wildcard when security is enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
          PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS,
          "alluxio.security.group.provider.IdentityUserGroupsMapping",
          PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP,
          "test_user_lsWildcard"})
  public void lsWildcard() throws Exception {
    String testUser = "test_user_lsWildcard";
    clearAndLogin(testUser);

    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);

    String expect = "";
    expect += getLsResultStr(new AlluxioURI(testDir + "/bar/foobar3"), 30, testUser, testUser);
    expect += getLsResultStr(new AlluxioURI(testDir + "/foo/foobar1"), 10, testUser, testUser);
    expect += getLsResultStr(new AlluxioURI(testDir + "/foo/foobar2"), 20, testUser, testUser);
    mFsShell.run("ls", testDir + "/*/foo*");
    Assert.assertEquals(expect, mOutput.toString());

    expect += getLsResultStr(new AlluxioURI(testDir + "/bar/foobar3"), 30, testUser, testUser);
    expect += getLsResultStr(new AlluxioURI(testDir + "/foo/foobar1"), 10, testUser, testUser);
    expect += getLsResultStr(new AlluxioURI(testDir + "/foo/foobar2"), 20, testUser, testUser);
    expect += getLsResultStr(new AlluxioURI(testDir + "/foobar4"), 40, testUser, testUser);
    mFsShell.run("ls", testDir + "/*");
    Assert.assertEquals(expect, mOutput.toString());
  }

  /**
   * Tests lsr command with wildcard when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsrNoAcl() throws IOException, AlluxioException {
    URIStatus[] files = createFiles();
    mFsShell.run("lsr", "/testRoot");
    String expected = "";
    expected += "WARNING: lsr is deprecated. Please use ls -R instead.\n";
    expected += getLsNoAclResultStr("/testRoot/testDir", files[1].getLastModificationTimeMs(),
        1, LsCommand.IN_ALLUXIO_STATE_DIR, files[1].getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testDir/testFileB",
        files[2].getLastModificationTimeMs(), 20, STATE_FILE_IN_ALLUXIO,
        files[2].getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileA", files[0].getLastModificationTimeMs(),
        10, STATE_FILE_IN_ALLUXIO, files[0].getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileC", files[3].getLastModificationTimeMs(),
        30, STATE_FILE_NOT_IN_ALLUXIO, files[3].getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests lsr command with wildcard when security is enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
          PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS,
          "alluxio.security.group.provider.IdentityUserGroupsMapping",
          PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP,
          "test_user_lsr"})
  public void lsr() throws Exception {
    String testUser = "test_user_lsr";
    clearAndLogin(testUser);

    URIStatus[] files = createFiles();
    mFsShell.run("lsr", "/testRoot");
    String expected = "";
    expected += "WARNING: lsr is deprecated. Please use ls -R instead.\n";
    expected +=
        getLsResultStr("/testRoot/testDir", files[1].getLastModificationTimeMs(), 1,
            LsCommand.IN_ALLUXIO_STATE_DIR, testUser, testUser, files[1].getMode(), false,
            files[1].isFolder(), files[1].getPersistenceState());
    expected += getLsResultStr("/testRoot/testDir/testFileB", files[2].getLastModificationTimeMs(),
        20, STATE_FILE_IN_ALLUXIO, testUser, testUser, files[2].getMode(), false,
        files[2].isFolder(), files[2].getPersistenceState());
    expected += getLsResultStr("/testRoot/testFileA", files[0].getLastModificationTimeMs(), 10,
        STATE_FILE_IN_ALLUXIO, testUser, testUser, files[0].getMode(), false,
        files[0].isFolder(), files[1].getPersistenceState());
    expected += getLsResultStr("/testRoot/testFileC", files[3].getLastModificationTimeMs(), 30,
        STATE_FILE_NOT_IN_ALLUXIO, testUser, testUser, files[3].getMode(), false,
        files[3].isFolder(), files[3].getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls command with a file where the file name includes a specifier character.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsWithFormatSpecifierCharacter() throws IOException, AlluxioException {
    String fileName = "/localhost%2C61764%2C1476207067267..meta.1476207073442.meta";
    FileSystemTestUtils.createByteFile(mFileSystem, fileName, WriteType.MUST_CACHE, 10);
    URIStatus file = mFileSystem.getStatus(new AlluxioURI(fileName));
    mFsShell.run("ls", "/");
    String expected = getLsNoAclResultStr(fileName, file.getLastModificationTimeMs(), 10,
        STATE_FILE_IN_ALLUXIO, file.getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls command with sort by path option.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
          confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
                  PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsWithSortByPath() throws IOException, AlluxioException {
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testLongFile", WriteType.MUST_CACHE, 100);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileZ", WriteType.MUST_CACHE, 10);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.MUST_CACHE, 50);
    URIStatus testFileA = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileA"));
    URIStatus testFileZ = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileZ"));
    URIStatus testLongFile = mFileSystem.getStatus(new AlluxioURI("/testRoot/testLongFile"));
    mFsShell.run("ls", "--sort", "path", "/testRoot");
    String expected = "";
    expected += getLsNoAclResultStr("/testRoot/testFileA", testFileA.getLastModificationTimeMs(),
        50, STATE_FILE_IN_ALLUXIO, testFileA.getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileZ", testFileZ.getLastModificationTimeMs(),
        10, STATE_FILE_IN_ALLUXIO, testFileZ.getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testLongFile",
            testLongFile.getLastModificationTimeMs(), 100,
            STATE_FILE_IN_ALLUXIO, testLongFile.getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls command with sort by size option.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
          confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
                  PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsWithSortBySize() throws IOException, AlluxioException {
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.MUST_CACHE, 50, 50);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileZ", WriteType.MUST_CACHE, 10, 10);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testLongFile", WriteType.MUST_CACHE, 100, 100);
    URIStatus testFileA = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileA"));
    URIStatus testFileZ = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileZ"));
    URIStatus testLongFile = mFileSystem.getStatus(new AlluxioURI("/testRoot/testLongFile"));
    mFsShell.run("ls", "--sort", "size", "/testRoot");
    String expected = "";
    expected += getLsNoAclResultStr("/testRoot/testFileZ", testFileZ.getLastModificationTimeMs(),
        10, STATE_FILE_IN_ALLUXIO, testFileZ.getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileA", testFileA.getLastModificationTimeMs(),
        50, STATE_FILE_IN_ALLUXIO, testFileA.getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testLongFile",
            testLongFile.getLastModificationTimeMs(), 100,
            STATE_FILE_IN_ALLUXIO, testLongFile.getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls command with sort by size and reverse order option.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
          confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
                  PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsWithSortBySizeAndReverse() throws IOException, AlluxioException {
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.MUST_CACHE, 50, 50);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileZ", WriteType.MUST_CACHE, 10, 10);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testLongFile", WriteType.MUST_CACHE, 100, 100);
    URIStatus testFileA = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileA"));
    URIStatus testFileZ = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileZ"));
    URIStatus testLongFile = mFileSystem.getStatus(new AlluxioURI("/testRoot/testLongFile"));
    mFsShell.run("ls", "--sort", "size", "-r", "/testRoot");
    String expected = "";
    expected += getLsNoAclResultStr("/testRoot/testLongFile",
        testLongFile.getLastModificationTimeMs(), 100, STATE_FILE_IN_ALLUXIO,
        testLongFile.getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileA", testFileA.getLastModificationTimeMs(),
        50, STATE_FILE_IN_ALLUXIO, testFileA.getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileZ", testFileZ.getLastModificationTimeMs(),
        10, STATE_FILE_IN_ALLUXIO, testFileZ.getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls command with an invalid sort option.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
          confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
                  PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsWithInvalidSortOption() throws IOException, AlluxioException {
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.MUST_CACHE, 50, 50);
    mFsShell.run("ls", "--sort", "unknownfield", "/testRoot");
    String expected = "Invalid sort option `unknownfield` for --sort\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls command with reverse sort order option.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
          confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
                  PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsReverseWithoutSort() throws IOException, AlluxioException {
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.MUST_CACHE, 50, 50);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileZ", WriteType.MUST_CACHE, 10, 10);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testLongFile", WriteType.MUST_CACHE, 100, 100);
    URIStatus testFileA = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileA"));
    URIStatus testFileZ = mFileSystem.getStatus(new AlluxioURI("/testRoot/testFileZ"));
    URIStatus testLongFile = mFileSystem.getStatus(new AlluxioURI("/testRoot/testLongFile"));
    mFsShell.run("ls", "-r", "/testRoot");
    String expected = "";
    expected += getLsNoAclResultStr("/testRoot/testLongFile",
        testLongFile.getLastModificationTimeMs(), 100, STATE_FILE_IN_ALLUXIO,
        testLongFile.getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileZ", testFileZ.getLastModificationTimeMs(),
        10, STATE_FILE_IN_ALLUXIO, testFileZ.getPersistenceState());
    expected += getLsNoAclResultStr("/testRoot/testFileA", testFileA.getLastModificationTimeMs(),
        50, STATE_FILE_IN_ALLUXIO, testFileA.getPersistenceState());
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
          PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS,
          "alluxio.security.group.provider.IdentityUserGroupsMapping",
          PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test_user_extended"})
  public void lsWithExtendedAcl() throws IOException, AlluxioException {
    String testUser = "test_user_extended";
    int size = 50;
    int indexOfExtended = 10; // index 10 of a line will be a '+'
    clearAndLogin(testUser);

    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testDir/testFileB", WriteType.MUST_CACHE, 20);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testFile", WriteType.MUST_CACHE, size, size);

    mFsShell.run("ls", "--sort", "path", "/testRoot");

    String line = "";
    String expected = "";
    line = getLsResultStr(new AlluxioURI("/testRoot/testDir"), 1, testUser, testUser);
    Assert.assertTrue(line.substring(0, 11).indexOf("+") != indexOfExtended);
    expected += line;
    line = getLsResultStr(new AlluxioURI("/testRoot/testFile"), size, testUser, testUser);
    Assert.assertTrue(line.substring(0, 11).indexOf("+") != indexOfExtended);
    expected += line;

    Assert.assertEquals(expected, mOutput.toString());

    mFileSystem.setAcl(new AlluxioURI("/testRoot/testDir"), SetAclAction.MODIFY,
        Arrays.asList(AclEntry.fromCliString("default:user:nameduser:rwx")),
        SetAclOptions.defaults());
    mFileSystem.setAcl(new AlluxioURI("/testRoot/testFile"), SetAclAction.MODIFY,
        Arrays.asList(AclEntry.fromCliString("user:nameduser:rwx")), SetAclOptions.defaults());

    mFsShell.run("ls", "--sort", "path", "/testRoot");

    line = getLsResultStr(new AlluxioURI("/testRoot/testDir"), 1, testUser, testUser);
    Assert.assertTrue(line.substring(0, 11).indexOf("+") == indexOfExtended);
    expected += line;
    line = getLsResultStr(new AlluxioURI("/testRoot/testFile"), size, testUser, testUser);
    Assert.assertTrue(line.substring(0, 11).indexOf("+") == indexOfExtended);
    expected += line;

    Assert.assertEquals(expected, mOutput.toString());
  }

}
