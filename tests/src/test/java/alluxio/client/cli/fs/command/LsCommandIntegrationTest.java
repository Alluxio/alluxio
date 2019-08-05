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

import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import alluxio.AlluxioURI;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.security.authorization.AclEntry;
import alluxio.security.user.TestUserState;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

/**
 * Tests for ls command.
 */
public final class LsCommandIntegrationTest extends AbstractFileSystemShellTest {
  // Helper function to create a set of files in the file system
  private void createFiles(String user) throws Exception {
    FileSystem fs = mFileSystem;
    if (user != null) {
      fs = mLocalAlluxioCluster.getClient(FileSystemContext
          .create(new TestUserState(user, ServerConfiguration.global()).getSubject(),
              ServerConfiguration.global()));
    }
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFileA", WritePType.MUST_CACHE, 10);
    FileSystemTestUtils
        .createByteFile(fs, "/testRoot/testDir/testFileB", WritePType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFileC", WritePType.THROUGH, 30);
  }

  /**
   * Tests ls command when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsNoAcl() throws Exception {
    createFiles(null);
    mFsShell.run("ls", "/testRoot");
    checkOutput(
        "              1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "             30       PERSISTED .+ .+   0% /testRoot/testFileC");
  }

  /**
   * Tests ls -h command when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsHumanReadable() throws Exception {
    createFiles(null);
    mFsShell.run("ls", "-h", "/testRoot");
    checkOutput(
        "              1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "            10B   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "            30B       PERSISTED .+ .+   0% /testRoot/testFileC");
  }

  /**
   * Tests ls -p command when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsPinned() throws Exception {
    createFiles(null);
    AlluxioURI fileURI1 = new AlluxioURI("/testRoot/testDir/testFileB");
    AlluxioURI fileURI2 = new AlluxioURI("/testRoot/testFileA");
    mFileSystem.setAttribute(fileURI1,
        SetAttributePOptions.newBuilder().setPinned(true).build());
    mFileSystem.setAttribute(fileURI2,
        SetAttributePOptions.newBuilder().setPinned(true).build());
    mFsShell.run("ls", "-pR",  "/testRoot");
    checkOutput(
        "             20   NOT_PERSISTED .+ .+ 100% /testRoot/testDir/testFileB",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA");
  }

  /**
   * Tests ls -d command when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsDirectoryAsPlainFileNoAcl() throws Exception {
    createFiles(null);
    mFsShell.run("ls", "-d", "/testRoot");
    checkOutput("              3       PERSISTED .+ .+  DIR /testRoot");
  }

  /**
   * Tests ls -d command on root directory when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsRootNoAcl() throws Exception {
    mFsShell.run("ls", "-d", "/");
    checkOutput("              0       PERSISTED .+ .+  DIR /    ");
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
    createFiles("test_user_ls");
    mFsShell.run("ls", "/testRoot");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "drwxr-xr-x  test_user_ls   test_user_ls                 1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "-rw-r--r--  test_user_ls   test_user_ls                10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "-rw-r--r--  test_user_ls   test_user_ls                30       PERSISTED .+ .+   0% /testRoot/testFileC");
    // CHECKSTYLE.ON: LineLengthExceed
  }

  /**
   * Tests ls command with wildcard when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsWildcardNoAcl() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(mFileSystem);

    mFsShell.run("ls", testDir + "/*/foo*");
    checkOutput(
        "             30   NOT_PERSISTED .+ .+ 100% /testDir/bar/foobar3",
        "             10   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar1",
        "             20   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar2");

    mOutput.reset();

    mFsShell.run("ls", testDir + "/*");
    checkOutput(
        "             30   NOT_PERSISTED .+ .+ 100% /testDir/bar/foobar3",
        "             10   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar1",
        "             20   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar2",
        "             40   NOT_PERSISTED .+ .+ 100% /testDir/foobar4");
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
    FileSystem fs = mLocalAlluxioCluster.getClient(FileSystemContext.create(
        new TestUserState("test_user_lsWildcard", ServerConfiguration.global()).getSubject(),
        ServerConfiguration.global()));

    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(fs);
    mFsShell.run("ls", testDir + "/*/foo*");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "-rw-r--r--  test_user_lsWildcardtest_user_lsWildcard             30   NOT_PERSISTED .+ .+ 100% /testDir/bar/foobar3",
        "-rw-r--r--  test_user_lsWildcardtest_user_lsWildcard             10   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar1",
        "-rw-r--r--  test_user_lsWildcardtest_user_lsWildcard             20   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar2");
    mOutput.reset();

    mFsShell.run("ls", testDir + "/*");
    checkOutput(
        "-rw-r--r--  test_user_lsWildcardtest_user_lsWildcard             30   NOT_PERSISTED .+ .+ 100% /testDir/bar/foobar3",
        "-rw-r--r--  test_user_lsWildcardtest_user_lsWildcard             10   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar1",
        "-rw-r--r--  test_user_lsWildcardtest_user_lsWildcard             20   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar2",
        "-rw-r--r--  test_user_lsWildcardtest_user_lsWildcard             40   NOT_PERSISTED .+ .+ 100% /testDir/foobar4");
    // CHECKSTYLE.ON: LineLengthExceed
  }

  /**
   * Tests ls -R command with wildcard when security is not enabled.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsrNoAcl() throws Exception {
    createFiles(null);
    mFsShell.run("ls", "-R", "/testRoot");
    checkOutput(
        "              1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "             20   NOT_PERSISTED .+ .+ 100% /testRoot/testDir/testFileB",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "             30       PERSISTED .+ .+   0% /testRoot/testFileC");
  }

  /**
   * Tests ls -R command with wildcard when security is enabled.
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
    createFiles("test_user_lsr");
    mFsShell.run("ls", "-R", "/testRoot");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "drwxr-xr-x  test_user_lsr  test_user_lsr                1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "-rw-r--r--  test_user_lsr  test_user_lsr               20   NOT_PERSISTED .+ .+ 100% /testRoot/testDir/testFileB",
        "-rw-r--r--  test_user_lsr  test_user_lsr               10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "-rw-r--r--  test_user_lsr  test_user_lsr               30       PERSISTED .+ .+   0% /testRoot/testFileC");
    // CHECKSTYLE.ON: LineLengthExceed
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
    FileSystemTestUtils.createByteFile(mFileSystem, fileName, WritePType.MUST_CACHE, 10);
    mFsShell.run("ls", "/");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput("             10   NOT_PERSISTED .+ .+ 100% /localhost%2C61764%2C1476207067267..meta.1476207073442.meta");
    // CHECKSTYLE.ON: LineLengthExceed
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
            .createByteFile(mFileSystem, "/testRoot/testLongFile", WritePType.MUST_CACHE, 100);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileZ", WritePType.MUST_CACHE, 10);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileA", WritePType.MUST_CACHE, 50);
    mFsShell.run("ls", "--sort", "path", "/testRoot");
    checkOutput(
        "             50   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileZ",
        "            100   NOT_PERSISTED .+ .+ 100% /testRoot/testLongFile");
  }

  /**
   * Tests ls command with sort by access time option.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL",
          PropertyKey.Name.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION, "0"})
  public void lsWithSortByAccessTime() throws IOException, AlluxioException {
    String oldFileRecentlyAccessed = "/testRoot/testFileRecent";
    String oldFileName = "/testRoot/testFile";
    FileSystemTestUtils
        .createByteFile(mFileSystem, oldFileRecentlyAccessed, WritePType.MUST_CACHE, 10);
    FileSystemTestUtils
        .createByteFile(mFileSystem, oldFileName, WritePType.MUST_CACHE, 10);

    FileSystemTestUtils.loadFile(mFileSystem, oldFileRecentlyAccessed);
    mFsShell.run("ls", "--sort", "lastAccessTime", "/testRoot");
    checkOutput(
        "             10   NOT_PERSISTED .+ .+ 100% " + oldFileName,
        "             10   NOT_PERSISTED .+ .+ 100% " + oldFileRecentlyAccessed);
  }

  /**
   * Tests ls command with sort by creation time option.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL",
          PropertyKey.Name.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION, "0"})
  public void lsWithSortByCreationTime() throws IOException, AlluxioException {
    String oldFileRecentlyAccessed = "/testRoot/testFileRecent";
    String oldFileName = "/testRoot/testFile";
    FileSystemTestUtils
        .createByteFile(mFileSystem, oldFileRecentlyAccessed, WritePType.MUST_CACHE, 10);
    FileSystemTestUtils
        .createByteFile(mFileSystem, oldFileName, WritePType.MUST_CACHE, 10);

    FileSystemTestUtils.loadFile(mFileSystem, oldFileRecentlyAccessed);
    mFsShell.run("ls", "--sort", "creationTime", "/testRoot");
    checkOutput(
        "             10   NOT_PERSISTED .+ .+ 100% " + oldFileRecentlyAccessed,
        "             10   NOT_PERSISTED .+ .+ 100% " + oldFileName);
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
            .createByteFile(mFileSystem, "/testRoot/testFileA", WritePType.MUST_CACHE, 50, 50);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileZ", WritePType.MUST_CACHE, 10, 10);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testLongFile", WritePType.MUST_CACHE, 100, 100);
    mFsShell.run("ls", "--sort", "size", "/testRoot");
    checkOutput(
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileZ",
        "             50   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "            100   NOT_PERSISTED .+ .+ /testRoot/testLongFile");
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
            .createByteFile(mFileSystem, "/testRoot/testFileA", WritePType.MUST_CACHE, 50, 50);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileZ", WritePType.MUST_CACHE, 10, 10);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testLongFile", WritePType.MUST_CACHE, 100, 100);
    mFsShell.run("ls", "--sort", "size", "-r", "/testRoot");
    checkOutput(
        "            100   NOT_PERSISTED .+ .+ 100% /testRoot/testLongFile",
        "             50   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileZ");
  }

  /**
   * Tests ls command with an invalid sort option.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
          confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
                  PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL"})
  public void lsWithInvalidSortOption() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testFileA",
        WritePType.MUST_CACHE, 50, 50);
    mFsShell.run("ls", "--sort", "unknownfield", "/testRoot");
    String expected = "Invalid sort option `unknownfield` for --sort\n";
    assertEquals(expected, mOutput.toString());
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
            .createByteFile(mFileSystem, "/testRoot/testFileA", WritePType.MUST_CACHE, 50, 50);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testFileZ", WritePType.MUST_CACHE, 10, 10);
    FileSystemTestUtils
            .createByteFile(mFileSystem, "/testRoot/testLongFile", WritePType.MUST_CACHE, 100, 100);
    mFsShell.run("ls", "-r", "/testRoot");
    checkOutput(
        "            100   NOT_PERSISTED .+ .+ 100% /testRoot/testLongFile",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileZ",
        "             50   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA");
  }

  private String getDisplayTime(long timestamp) {
    String formatString = ServerConfiguration.get(PropertyKey.USER_DATE_FORMAT_PATTERN);
    return CommonUtils.convertMsToDate(timestamp, formatString);
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
          PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS,
          "alluxio.security.group.provider.IdentityUserGroupsMapping",
          PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test_user_ls",
          PropertyKey.Name.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION, "0"
      })
  public void lsWithCreationTime() throws Exception {
    checkLsWithTimestamp("creationTime", URIStatus::getCreationTimeMs);
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
          PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS,
          "alluxio.security.group.provider.IdentityUserGroupsMapping",
          PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test_user_ls",
          PropertyKey.Name.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION, "0"
      })
  public void lsWithModificationTime() throws Exception {
    checkLsWithTimestamp("lastModificationTime", URIStatus::getLastModificationTimeMs);
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
          PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS,
          "alluxio.security.group.provider.IdentityUserGroupsMapping",
          PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test_user_ls",
          PropertyKey.Name.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION, "0"
      })
  public void lsWithAccessTime() throws Exception {
    checkLsWithTimestamp("lastAccessTime", URIStatus::getLastAccessTimeMs);
  }

  private void checkLsWithTimestamp(String parameter, Function<URIStatus, Long> timestampFunc)
      throws Exception {
    createFiles("test_user_ls");
    String testDir = "/testRoot/testDir";
    String testFileA = "/testRoot/testFileA";
    String testFileC = "/testRoot/testFileC";
    mFileSystem.listStatus(new AlluxioURI(testDir));
    FileSystemTestUtils.loadFile(mFileSystem, testFileA);
    FileSystemTestUtils.loadFile(mFileSystem, testFileC);
    mFsShell.run("ls", "--timestamp", parameter, "/testRoot");
    long time1 = timestampFunc.apply(mFileSystem.getStatus(new AlluxioURI(testDir)));
    long time2 = timestampFunc.apply(mFileSystem.getStatus(new AlluxioURI(testFileA)));
    long time3 = timestampFunc.apply(mFileSystem.getStatus(new AlluxioURI(testFileC)));
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "drwxr-xr-x  test_user_ls   test_user_ls                 1   NOT_PERSISTED " +  getDisplayTime(time1) + "  DIR /testRoot/testDir",
        "-rw-r--r--  test_user_ls   test_user_ls                10   NOT_PERSISTED " +  getDisplayTime(time2) + " 100% /testRoot/testFileA",
        "-rw-r--r--  test_user_ls   test_user_ls                30       PERSISTED " +  getDisplayTime(time3) + " 100% /testRoot/testFileC");
    // CHECKSTYLE.ON: LineLengthExceed
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
          PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
          PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS,
          "alluxio.security.group.provider.IdentityUserGroupsMapping",
          PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test_user_extended"})
  public void lsWithExtendedAcl() throws IOException, AlluxioException {
    int size = 50;

    FileSystem fs = mLocalAlluxioCluster.getClient(FileSystemContext
        .create(new TestUserState("test_user_extended", ServerConfiguration.global()).getSubject(),
            ServerConfiguration.global()));
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testDir/testFileB",
        WritePType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFile",
        WritePType.MUST_CACHE, size, size);

    mFsShell.run("ls", "--sort", "path", "/testRoot");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "drwxr-xr-x  test_user_extendedtest_user_extended              1   NOT_PERSISTED .+ .+ DIR /testRoot/testDir",
        "-rw-r--r--  test_user_extendedtest_user_extended             50   NOT_PERSISTED .+ .+ 100% /testRoot/testFile");
    // CHECKSTYLE.ON: LineLengthExceed
    mOutput.reset();

    fs.setAcl(new AlluxioURI("/testRoot/testDir"), SetAclAction.MODIFY,
        Arrays.asList(AclEntry.fromCliString("default:user:nameduser:rwx")));
    fs.setAcl(new AlluxioURI("/testRoot/testFile"), SetAclAction.MODIFY,
        Arrays.asList(AclEntry.fromCliString("user:nameduser:rwx")));

    mFsShell.run("ls", "--sort", "path", "/testRoot");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "drwxr-xr-x\\+ test_user_extendedtest_user_extended              1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "-rw-r--r--\\+ test_user_extendedtest_user_extended             50   NOT_PERSISTED .+ .+ 100% /testRoot/testFile");
    // CHECKSTYLE.ON: LineLengthExceed
  }

  private void checkOutput(String... linePatterns) {
    String[] actualLines = mOutput.toString().split("\n");
    assertEquals("Output: " + mOutput.toString(), linePatterns.length, actualLines.length);
    for (int i = 0; i < linePatterns.length; i++) {
      assertThat("mOutput: " + mOutput.toString(), actualLines[i], matchesPattern(linePatterns[i]));
    }
  }
}
