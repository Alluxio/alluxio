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
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;
import alluxio.util.FormatUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for ls command.
 */
public final class LsCommandTest extends AbstractAlluxioShellTest {
  // Helper function to format ls result.
  private String getLsResultStr(AlluxioURI uri, int size, String testUser, String testGroup)
      throws IOException, AlluxioException {
    URIStatus status = mFileSystem.getStatus(uri);
    return getLsResultStr(uri.getPath(), status.getCreationTimeMs(), size,
        LsCommand.STATE_FILE_IN_MEMORY, testUser, testGroup, status.getMode(),
        status.isFolder());
  }

  // Helper function to format ls result.
  private String getLsResultStr(String path, long createTime, int size, String fileType,
      String testUser, String testGroup, int permission, boolean isDir)
      throws IOException, AlluxioException {
    return String
        .format(Constants.LS_FORMAT, FormatUtils.formatMode((short) permission, isDir),
            testUser, testGroup, FormatUtils.getSizeFromBytes(size),
            CommandUtils.convertMsToDate(createTime), fileType, path);
  }

  // Helper function to format ls result without acl enabled.
  private String getLsNoAclResultStr(AlluxioURI uri, int size, String fileType)
      throws IOException, AlluxioException {
    URIStatus status = mFileSystem.getStatus(uri);
    return getLsNoAclResultStr(uri.getPath(), status.getCreationTimeMs(), size, fileType);
  }

  // Helper function to format ls result without acl enabled.
  private String getLsNoAclResultStr(String path, long createTime, int size, String fileType)
      throws IOException, AlluxioException {
    return String.format(Constants.LS_FORMAT_NO_ACL, FormatUtils.getSizeFromBytes(size),
        CommandUtils.convertMsToDate(createTime), fileType, path);
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
    expected += getLsNoAclResultStr("/testRoot/testFileA", files[0].getCreationTimeMs(), 10,
        LsCommand.STATE_FILE_IN_MEMORY);
    expected += getLsNoAclResultStr("/testRoot/testDir", files[1].getCreationTimeMs(), 1,
        LsCommand.STATE_FOLDER);
    expected += getLsNoAclResultStr("/testRoot/testFileC", files[3].getCreationTimeMs(), 30,
        LsCommand.STATE_FILE_NOT_IN_MEMORY);
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
    mFileSystem.setAttribute(new AlluxioURI("/testRoot/testFileA"),
        SetAttributeOptions.defaults().setPinned(true));
    mFileSystem.setAttribute(new AlluxioURI("/testRoot/testDir/testFileB"),
        SetAttributeOptions.defaults().setPinned(true));
    mFsShell.run("ls", "-pR",  "/testRoot");
    String expected = "";
    expected += getLsNoAclResultStr("/testRoot/testFileA", files[0].getCreationTimeMs(), 10,
        LsCommand.STATE_FILE_IN_MEMORY);
    expected += getLsNoAclResultStr("/testRoot/testDir/testFileB", files[2].getCreationTimeMs(), 20,
        LsCommand.STATE_FILE_IN_MEMORY);
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
    expected += getLsNoAclResultStr("/testRoot", dirStatus.getCreationTimeMs(),
        3 /* number of direct children under /testRoot/ dir */, LsCommand.STATE_FOLDER);
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
    expected += getLsNoAclResultStr("/", dirStatus.getCreationTimeMs(), 0, LsCommand.STATE_FOLDER);
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
    expected += getLsResultStr("/testRoot/testFileA", files[0].getCreationTimeMs(), 10,
        LsCommand.STATE_FILE_IN_MEMORY, testUser, testUser, files[0].getMode(),
        files[0].isFolder());
    expected +=
        getLsResultStr("/testRoot/testDir", files[1].getCreationTimeMs(), 1, LsCommand.STATE_FOLDER,
            testUser, testUser, files[1].getMode(), files[1].isFolder());
    expected += getLsResultStr("/testRoot/testFileC", files[3].getCreationTimeMs(), 30,
        LsCommand.STATE_FILE_NOT_IN_MEMORY, testUser, testUser, files[3].getMode(),
        files[3].isFolder());
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
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);

    String expect = "";
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/bar/foobar3"), 30,
        LsCommand.STATE_FILE_IN_MEMORY);
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/foo/foobar1"), 10,
        LsCommand.STATE_FILE_IN_MEMORY);
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/foo/foobar2"), 20,
        LsCommand.STATE_FILE_IN_MEMORY);
    mFsShell.run("ls", testDir + "/*/foo*");
    Assert.assertEquals(expect, mOutput.toString());

    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/bar/foobar3"), 30,
        LsCommand.STATE_FILE_IN_MEMORY);
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/foo/foobar1"), 10,
        LsCommand.STATE_FILE_IN_MEMORY);
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/foo/foobar2"), 20,
        LsCommand.STATE_FILE_IN_MEMORY);
    expect += getLsNoAclResultStr(new AlluxioURI(testDir + "/foobar4"), 40,
        LsCommand.STATE_FILE_IN_MEMORY);
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

    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);

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
    expected += getLsNoAclResultStr("/testRoot/testFileA", files[0].getCreationTimeMs(), 10,
        LsCommand.STATE_FILE_IN_MEMORY);
    expected += getLsNoAclResultStr("/testRoot/testDir", files[1].getCreationTimeMs(), 1,
        LsCommand.STATE_FOLDER);
    expected += getLsNoAclResultStr("/testRoot/testDir/testFileB", files[2].getCreationTimeMs(), 20,
        LsCommand.STATE_FILE_IN_MEMORY);
    expected += getLsNoAclResultStr("/testRoot/testFileC", files[3].getCreationTimeMs(), 30,
        LsCommand.STATE_FILE_NOT_IN_MEMORY);
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
    expected += getLsResultStr("/testRoot/testFileA", files[0].getCreationTimeMs(), 10,
        LsCommand.STATE_FILE_IN_MEMORY, testUser, testUser, files[0].getMode(),
        files[0].isFolder());
    expected +=
        getLsResultStr("/testRoot/testDir", files[1].getCreationTimeMs(), 1, LsCommand.STATE_FOLDER,
            testUser, testUser, files[1].getMode(), files[1].isFolder());
    expected += getLsResultStr("/testRoot/testDir/testFileB", files[2].getCreationTimeMs(), 20,
        LsCommand.STATE_FILE_IN_MEMORY, testUser, testUser, files[2].getMode(),
        files[2].isFolder());
    expected += getLsResultStr("/testRoot/testFileC", files[3].getCreationTimeMs(), 30,
        LsCommand.STATE_FILE_NOT_IN_MEMORY, testUser, testUser, files[3].getMode(),
        files[3].isFolder());
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
    String expected = getLsNoAclResultStr(fileName, file.getCreationTimeMs(), 10,
        LsCommand.STATE_FILE_IN_MEMORY);
    Assert.assertEquals(expected, mOutput.toString());
  }
}
