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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.security.user.TestUserState;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Test;

import java.io.IOException;

/**
 * Tests for ls command.
 */
@LocalAlluxioClusterResource.ServerConfig(
    confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
        PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "NOSASL",
        PropertyKey.Name.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION, "0"})
public final class LsCommandIntegrationTest extends AbstractFileSystemShellTest {
  // Helper function to create a set of files in the file system
  private void createFiles(String user) throws Exception {
    FileSystem fs = sFileSystem;
    if (user != null) {
      fs = sLocalAlluxioCluster.getClient(FileSystemContext
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
  public void lsNoAcl() throws Exception {
    createFiles(null);
    sFsShell.run("ls", "--sort", "path", "/testRoot");
    checkOutput(
        "              1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "             30       PERSISTED .+ .+   0% /testRoot/testFileC");
  }

  /**
   * Tests ls command when arguments are multiple directories and security is not enabled.
   */
  @Test
  public void lsMultipleDirs() throws Exception {
    createFiles(null);
    sFsShell.run("ls", "--sort", "path", "/testRoot", "/testRoot/testDir/");
    checkOutput(
        "              1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "             30       PERSISTED .+ .+   0% /testRoot/testFileC",
        "             20   NOT_PERSISTED .+ .+ 100% /testRoot/testDir/testFileB");
  }

  /**
   * Tests ls -h command when security is not enabled.
   */
  @Test
  public void lsHumanReadable() throws Exception {
    createFiles(null);
    sFsShell.run("ls", "-h", "--sort", "path", "/testRoot");
    checkOutput(
        "              1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "            10B   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "            30B       PERSISTED .+ .+   0% /testRoot/testFileC");
  }

  /**
   * Tests ls -p command when security is not enabled.
   */
  @Test
  public void lsPinned() throws Exception {
    createFiles(null);
    AlluxioURI fileURI1 = new AlluxioURI("/testRoot/testDir/testFileB");
    AlluxioURI fileURI2 = new AlluxioURI("/testRoot/testFileA");
    sFileSystem.setAttribute(fileURI1,
        SetAttributePOptions.newBuilder().setPinned(true).build());
    sFileSystem.setAttribute(fileURI2,
        SetAttributePOptions.newBuilder().setPinned(true).build());
    sFsShell.run("ls", "-pR", "--sort", "path", "/testRoot");
    checkOutput(
        "             20   NOT_PERSISTED .+ .+ 100% /testRoot/testDir/testFileB",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA");
  }

  /**
   * Tests ls -d command when security is not enabled.
   */
  @Test
  public void lsDirectoryAsPlainFileNoAcl() throws Exception {
    createFiles(null);
    sFsShell.run("ls", "-d", "/testRoot");
    checkOutput("              3       PERSISTED .+ .+  DIR /testRoot");
  }

  /**
   * Tests ls -d command on root directory when security is not enabled.
   */
  @Test
  public void lsRootNoAcl() throws Exception {
    sFsShell.run("ls", "-d", "/");
    checkOutput("              0       PERSISTED .+ .+  DIR /    ");
  }

  /**
   * Tests ls command with wildcard when security is not enabled.
   */
  @Test
  public void lsWildcardNoAcl() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);

    sFsShell.run("ls", "--sort", "path", testDir + "/*/foo*");
    checkOutput(
        "             30   NOT_PERSISTED .+ .+ 100% /testDir/bar/foobar3",
        "             10   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar1",
        "             20   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar2");

    mOutput.reset();

    sFsShell.run("ls", "--sort", "path", testDir + "/*");
    checkOutput(
        "             30   NOT_PERSISTED .+ .+ 100% /testDir/bar/foobar3",
        "             10   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar1",
        "             20   NOT_PERSISTED .+ .+ 100% /testDir/foo/foobar2",
        "             40   NOT_PERSISTED .+ .+ 100% /testDir/foobar4");
  }

  /**
   * Tests ls -R command with wildcard when security is not enabled.
   */
  @Test
  public void lsrNoAcl() throws Exception {
    createFiles(null);
    sFsShell.run("ls", "-R", "--sort", "path", "/testRoot");
    checkOutput(
        "              1   NOT_PERSISTED .+ .+  DIR /testRoot/testDir",
        "             20   NOT_PERSISTED .+ .+ 100% /testRoot/testDir/testFileB",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "             30       PERSISTED .+ .+   0% /testRoot/testFileC");
  }

  /**
   * Tests ls command with a file where the file name includes a specifier character.
   */
  @Test
  public void lsWithFormatSpecifierCharacter() throws IOException, AlluxioException {
    String fileName = "/localhost%2C61764%2C1476207067267..meta.1476207073442.meta";
    FileSystemTestUtils.createByteFile(sFileSystem, fileName, WritePType.MUST_CACHE, 10);
    sFsShell.run("ls", "/");
    // CHECKSTYLE.OFF: LineLengthExceed - Improve readability
    checkOutput("             10   NOT_PERSISTED .+ .+ 100% /localhost%2C61764%2C1476207067267..meta.1476207073442.meta");
    // CHECKSTYLE.ON: LineLengthExceed
  }

  /**
   * Tests ls command with sort by path option.
   */
  @Test
  public void lsWithSortByPath() throws IOException, AlluxioException {
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testLongFile", WritePType.MUST_CACHE, 100);
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testFileZ", WritePType.MUST_CACHE, 10);
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testFileA", WritePType.MUST_CACHE, 50);
    sFsShell.run("ls", "--sort", "path", "/testRoot");
    checkOutput(
        "             50   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileZ",
        "            100   NOT_PERSISTED .+ .+ 100% /testRoot/testLongFile");
  }

  /**
   * Tests ls command with sort by access time option.
   */
  @Test
  public void lsWithSortByAccessTime() throws IOException, AlluxioException {
    String oldFileRecentlyAccessed = "/testRoot/testFileRecent";
    String oldFileName = "/testRoot/testFile";
    FileSystemTestUtils
        .createByteFile(sFileSystem, oldFileRecentlyAccessed, WritePType.MUST_CACHE, 10);
    FileSystemTestUtils
        .createByteFile(sFileSystem, oldFileName, WritePType.MUST_CACHE, 10);

    FileSystemTestUtils.loadFile(sFileSystem, oldFileRecentlyAccessed);
    sFsShell.run("ls", "--sort", "lastAccessTime", "/testRoot");
    checkOutput(
        "             10   NOT_PERSISTED .+ .+ 100% " + oldFileName,
        "             10   NOT_PERSISTED .+ .+ 100% " + oldFileRecentlyAccessed);
  }

  /**
   * Tests ls command with sort by creation time option.
   */
  @Test
  public void lsWithSortByCreationTime() throws IOException, AlluxioException {
    String oldFileRecentlyAccessed = "/testRoot/testFileRecent";
    String oldFileName = "/testRoot/testFile";
    FileSystemTestUtils
        .createByteFile(sFileSystem, oldFileRecentlyAccessed, WritePType.MUST_CACHE, 10);
    FileSystemTestUtils
        .createByteFile(sFileSystem, oldFileName, WritePType.MUST_CACHE, 10);

    FileSystemTestUtils.loadFile(sFileSystem, oldFileRecentlyAccessed);
    sFsShell.run("ls", "--sort", "creationTime", "/testRoot");
    checkOutput(
        "             10   NOT_PERSISTED .+ .+ 100% " + oldFileRecentlyAccessed,
        "             10   NOT_PERSISTED .+ .+ 100% " + oldFileName);
  }

  /**
   * Tests ls command with sort by size option.
   */
  @Test
  public void lsWithSortBySize() throws IOException, AlluxioException {
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testFileA", WritePType.MUST_CACHE, 50, 50);
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testFileZ", WritePType.MUST_CACHE, 10, 10);
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testLongFile", WritePType.MUST_CACHE, 100, 100);
    sFsShell.run("ls", "--sort", "size", "/testRoot");
    checkOutput(
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileZ",
        "             50   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "            100   NOT_PERSISTED .+ .+ /testRoot/testLongFile");
  }

  /**
   * Tests ls command with sort by size and reverse order option.
   */
  @Test
  public void lsWithSortBySizeAndReverse() throws IOException, AlluxioException {
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testFileA", WritePType.MUST_CACHE, 50, 50);
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testFileZ", WritePType.MUST_CACHE, 10, 10);
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testLongFile", WritePType.MUST_CACHE, 100, 100);
    sFsShell.run("ls", "--sort", "size", "-r", "/testRoot");
    checkOutput(
        "            100   NOT_PERSISTED .+ .+ 100% /testRoot/testLongFile",
        "             50   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileZ");
  }

  /**
   * Tests ls command with an invalid sort option.
   */
  @Test
  public void lsWithInvalidSortOption() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testFileA",
        WritePType.MUST_CACHE, 50, 50);
    sFsShell.run("ls", "--sort", "unknownfield", "/testRoot");
    String expected = "Invalid sort option `unknownfield` for --sort\n";
    assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests ls command with reverse sort order option.
   */
  @Test
  public void lsReverseWithoutSort() throws IOException, AlluxioException {
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testFileA", WritePType.MUST_CACHE, 50, 50);
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testFileZ", WritePType.MUST_CACHE, 10, 10);
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testLongFile", WritePType.MUST_CACHE, 100, 100);
    sFsShell.run("ls", "-r", "--sort", "path", "/testRoot");
    checkOutput(
        "            100   NOT_PERSISTED .+ .+ 100% /testRoot/testLongFile",
        "             10   NOT_PERSISTED .+ .+ 100% /testRoot/testFileZ",
        "             50   NOT_PERSISTED .+ .+ 100% /testRoot/testFileA");
  }

  private void checkOutput(String... linePatterns) {
    String[] actualLines = mOutput.toString().split("\n");
    assertEquals("Output: " + mOutput.toString(), linePatterns.length, actualLines.length);
    for (int i = 0; i < linePatterns.length; i++) {
      assertThat("mOutput: " + mOutput.toString(), actualLines[i], matchesPattern(linePatterns[i]));
    }
  }
}
