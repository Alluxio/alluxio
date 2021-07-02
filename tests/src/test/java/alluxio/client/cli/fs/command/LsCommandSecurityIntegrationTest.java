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
 * Tests for ls command, with security enabled.
 */
@LocalAlluxioClusterResource.ServerConfig(
    confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
        PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
        PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS,
        "alluxio.security.group.provider.IdentityUserGroupsMapping",
        PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test_user_ls",
        PropertyKey.Name.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION, "0"})
public final class LsCommandSecurityIntegrationTest extends AbstractFileSystemShellTest {
  // Helper function to create a set of files in the file system
  private void createFiles() throws Exception {
    FileSystem fs = sLocalAlluxioCluster.getClient(FileSystemContext
        .create(new TestUserState("test_user_ls", ServerConfiguration.global()).getSubject(),
            ServerConfiguration.global()));
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFileA", WritePType.MUST_CACHE, 10);
    FileSystemTestUtils
        .createByteFile(fs, "/testRoot/testDir/testFileB", WritePType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFileC", WritePType.THROUGH, 30);
  }

  /**
   * Tests ls command when security is enabled.
   */
  @Test
  public void ls() throws Exception {
    createFiles();
    sFsShell.run("ls", "--sort", "path", "/testRoot");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "drwxr-xr-x  test_user_ls   test_user_ls                 1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "-rw-r--r--  test_user_ls   test_user_ls                10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "-rw-r--r--  test_user_ls   test_user_ls                30       PERSISTED .+ .+   0% /testRoot/testFileC");
    // CHECKSTYLE.ON: LineLengthExceed
  }

  /**
   * Tests ls command with wildcard when security is enabled.
   */
  @Test
  public void lsWildcard() throws Exception {
    FileSystem fs = sLocalAlluxioCluster.getClient(FileSystemContext.create(
        new TestUserState("test_user_ls", ServerConfiguration.global()).getSubject(),
        ServerConfiguration.global()));

    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(fs);
    sFsShell.run("ls", testDir + "/*/foo*");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "-rw-r--r--  test_user_ls   test_user_ls                30   NOT_PERSISTED .+ .+ 100% /testDir/bar/foobar3",
        "-rw-r--r--  test_user_ls   test_user_ls                10   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar1",
        "-rw-r--r--  test_user_ls   test_user_ls                20   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar2");
    mOutput.reset();

    sFsShell.run("ls", "--sort", "path", testDir + "/*");
    checkOutput(
        "-rw-r--r--  test_user_ls   test_user_ls                30   NOT_PERSISTED .+ .+ 100% /testDir/bar/foobar3",
        "-rw-r--r--  test_user_ls   test_user_ls                10   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar1",
        "-rw-r--r--  test_user_ls   test_user_ls                20   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar2",
        "-rw-r--r--  test_user_ls   test_user_ls                40   NOT_PERSISTED .+ .+ 100% /testDir/foobar4");
    // CHECKSTYLE.ON: LineLengthExceed
  }

  /**
   * Tests ls -R command with wildcard when security is enabled.
   */
  @Test
  public void lsr() throws Exception {
    createFiles();
    sFsShell.run("ls", "-R", "--sort", "path", "/testRoot");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "drwxr-xr-x  test_user_ls   test_user_ls                 1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "-rw-r--r--  test_user_ls   test_user_ls                20   NOT_PERSISTED .+ .+ 100% /testRoot/testDir/testFileB",
        "-rw-r--r--  test_user_ls   test_user_ls                10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "-rw-r--r--  test_user_ls   test_user_ls                30       PERSISTED .+ .+   0% /testRoot/testFileC");
    // CHECKSTYLE.ON: LineLengthExceed
  }

  private String getDisplayTime(long timestamp) {
    String formatString = ServerConfiguration.get(PropertyKey.USER_DATE_FORMAT_PATTERN);
    return CommonUtils.convertMsToDate(timestamp, formatString);
  }

  @Test
  public void lsWithCreationTime() throws Exception {
    checkLsWithTimestamp("creationTime", URIStatus::getCreationTimeMs);
  }

  @Test
  public void lsWithModificationTime() throws Exception {
    checkLsWithTimestamp("lastModificationTime", URIStatus::getLastModificationTimeMs);
  }

  @Test
  public void lsWithAccessTime() throws Exception {
    checkLsWithTimestamp("lastAccessTime", URIStatus::getLastAccessTimeMs);
  }

  private void checkLsWithTimestamp(String parameter, Function<URIStatus, Long> timestampFunc)
      throws Exception {
    createFiles();
    String testDir = "/testRoot/testDir";
    String testFileA = "/testRoot/testFileA";
    String testFileC = "/testRoot/testFileC";
    sFileSystem.listStatus(new AlluxioURI(testDir));
    FileSystemTestUtils.loadFile(sFileSystem, testFileA);
    FileSystemTestUtils.loadFile(sFileSystem, testFileC);
    sFsShell.run("ls", "--timestamp", parameter, "--sort", "path", "/testRoot");
    long time1 = timestampFunc.apply(sFileSystem.getStatus(new AlluxioURI(testDir)));
    long time2 = timestampFunc.apply(sFileSystem.getStatus(new AlluxioURI(testFileA)));
    long time3 = timestampFunc.apply(sFileSystem.getStatus(new AlluxioURI(testFileC)));
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "drwxr-xr-x  test_user_ls   test_user_ls                 1   NOT_PERSISTED " +  getDisplayTime(time1) + "  DIR /testRoot/testDir",
        "-rw-r--r--  test_user_ls   test_user_ls                10   NOT_PERSISTED " +  getDisplayTime(time2) + " 100% /testRoot/testFileA",
        "-rw-r--r--  test_user_ls   test_user_ls                30       PERSISTED " +  getDisplayTime(time3) + " 100% /testRoot/testFileC");
    // CHECKSTYLE.ON: LineLengthExceed
  }

  @Test
  public void lsWithExtendedAcl() throws IOException, AlluxioException {
    int size = 50;

    FileSystem fs = sLocalAlluxioCluster.getClient(FileSystemContext
        .create(new TestUserState("test_user_ls", ServerConfiguration.global()).getSubject(),
            ServerConfiguration.global()));
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testDir/testFileB",
        WritePType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFile",
        WritePType.MUST_CACHE, size, size);

    sFsShell.run("ls", "--sort", "path", "/testRoot");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "drwxr-xr-x  test_user_ls   test_user_ls                 1   NOT_PERSISTED .+ .+ DIR /testRoot/testDir",
        "-rw-r--r--  test_user_ls   test_user_ls                50   NOT_PERSISTED .+ .+ 100% /testRoot/testFile");
    // CHECKSTYLE.ON: LineLengthExceed
    mOutput.reset();

    fs.setAcl(new AlluxioURI("/testRoot/testDir"), SetAclAction.MODIFY,
        Arrays.asList(AclEntry.fromCliString("default:user:nameduser:rwx")));
    fs.setAcl(new AlluxioURI("/testRoot/testFile"), SetAclAction.MODIFY,
        Arrays.asList(AclEntry.fromCliString("user:nameduser:rwx")));

    sFsShell.run("ls", "--sort", "path", "/testRoot");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput(
        "drwxr-xr-x\\+ test_user_ls   test_user_ls                 1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "-rw-r--r--\\+ test_user_ls   test_user_ls                50   NOT_PERSISTED .+ .+ 100% /testRoot/testFile");
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
