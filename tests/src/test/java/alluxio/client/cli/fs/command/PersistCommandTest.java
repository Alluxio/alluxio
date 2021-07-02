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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.TestLoggerRule;
import alluxio.cli.fs.FileSystemShell;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.exception.ExceptionMessage;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.security.authorization.Mode;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for persist command.
 */
@LocalAlluxioClusterResource.ServerConfig(
    confParams = {PropertyKey.Name.MASTER_PERSISTENCE_BLACKLIST, "foobar_blacklist"})
public final class PersistCommandTest extends AbstractFileSystemShellTest {

  @Rule
  public TestLoggerRule mLogRule = new TestLoggerRule();

  @Test
  public void persist() throws Exception {
    String testFilePath = "/testPersist/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, testFilePath, WritePType.MUST_CACHE, 10);
    assertFalse(sFileSystem.getStatus(new AlluxioURI("/testPersist/testFile")).isPersisted());

    int ret = sFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    checkFilePersisted(new AlluxioURI("/testPersist/testFile"), 10);
  }

  @Test
  public void persistDirectory() throws Exception {
    // Set the default write type to MUST_CACHE, so that directories are not persisted by default
    ServerConfiguration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    assertFalse(sFileSystem.getStatus(new AlluxioURI(testDir)).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(testDir + "/foo")).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(testDir + "/bar")).isPersisted());
    int ret = sFsShell.run("persist", testDir);
    Assert.assertEquals(0, ret);
    assertTrue(sFileSystem.getStatus(new AlluxioURI(testDir)).isPersisted());
    assertTrue(sFileSystem.getStatus(new AlluxioURI(testDir + "/foo")).isPersisted());
    assertTrue(sFileSystem.getStatus(new AlluxioURI(testDir + "/bar")).isPersisted());
    checkFilePersisted(new AlluxioURI(testDir + "/foo/foobar1"), 10);
    checkFilePersisted(new AlluxioURI(testDir + "/foo/foobar2"), 20);
    checkFilePersisted(new AlluxioURI(testDir + "/bar/foobar3"), 30);
    checkFilePersisted(new AlluxioURI(testDir + "/foobar4"), 40);
  }

  @Test
  public void persistOnRenameDirectory() throws Exception {
    InstancedConfiguration conf = new InstancedConfiguration(ServerConfiguration.global());
    conf.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    conf.set(PropertyKey.USER_FILE_PERSIST_ON_RENAME, "true");

    try (FileSystemShell fsShell = new FileSystemShell(conf)) {
      String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
      String toPersist = testDir + "/foo";
      String persisted = testDir + "/foo_persisted";
      String doNotPersist = testDir + "/bar";
      assertFalse(sFileSystem.getStatus(new AlluxioURI(testDir)).isPersisted());
      assertFalse(sFileSystem.getStatus(new AlluxioURI(toPersist)).isPersisted());
      assertFalse(sFileSystem.getStatus(new AlluxioURI(doNotPersist)).isPersisted());
      int ret = fsShell.run("mv", toPersist, persisted);
      Assert.assertEquals(mOutput.toString(), 0, ret);
      CommonUtils.waitFor("Directory to be persisted", () -> {
        try {
          return sFileSystem.getStatus(new AlluxioURI(persisted)).isPersisted()
              && sFileSystem.getStatus(new AlluxioURI(persisted + "/foobar1")).isPersisted()
              && sFileSystem.getStatus(new AlluxioURI(persisted + "/foobar2")).isPersisted();
        } catch (Exception e) {
          return false;
        }
      }, WaitForOptions.defaults().setTimeoutMs(10000));
      assertFalse(sFileSystem.getStatus(new AlluxioURI(testDir + "/bar")).isPersisted());
      checkFilePersisted(new AlluxioURI(persisted + "/foobar1"), 10);
      checkFilePersisted(new AlluxioURI(persisted + "/foobar2"), 20);
    }
  }

  @Test
  public void persistOnRenameDirectoryBlacklist() throws Exception {
    InstancedConfiguration conf = new InstancedConfiguration(ServerConfiguration.global());
    conf.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    conf.set(PropertyKey.USER_FILE_PERSIST_ON_RENAME, "true");
    // MASTER_PERSISTENCE_BLACKLIST is set to "foobar_blacklist" for the server configuration

    try (FileSystemShell fsShell = new FileSystemShell(conf)) {
      String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
      String toPersist = testDir + "/foo";
      String persisted = testDir + "/foo_persisted";
      // create the file that is blacklisted, under the directory
      FileSystemTestUtils
          .createByteFile(sFileSystem, toPersist + "/foobar_blacklist", WritePType.MUST_CACHE, 10);

      assertFalse(sFileSystem.getStatus(new AlluxioURI(testDir)).isPersisted());
      assertFalse(sFileSystem.getStatus(new AlluxioURI(toPersist)).isPersisted());
      assertFalse(
          sFileSystem.getStatus(new AlluxioURI(toPersist + "/foobar_blacklist")).isPersisted());
      int ret = fsShell.run("mv", toPersist, persisted);
      Assert.assertEquals(0, ret);
      CommonUtils.waitFor("Directory to be persisted", () -> {
        try {
          return sFileSystem.getStatus(new AlluxioURI(persisted)).isPersisted()
              && sFileSystem.getStatus(new AlluxioURI(persisted + "/foobar1")).isPersisted();
        } catch (Exception e) {
          return false;
        }
      }, WaitForOptions.defaults().setTimeoutMs(10000));
      assertFalse(
          sFileSystem.getStatus(new AlluxioURI(persisted + "/foobar_blacklist")).isPersisted());
      checkFilePersisted(new AlluxioURI(persisted + "/foobar1"), 10);
    }
  }

  @Test
  public void persistMultiFiles() throws Exception {
    String filePath1 = "/testPersist/testFile1";
    String filePath2 = "/testFile2";
    String filePath3 = "/testPersist/testFile3";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath1, WritePType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, filePath2, WritePType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(sFileSystem, filePath3, WritePType.MUST_CACHE, 30);

    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath1)).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath2)).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath3)).isPersisted());

    int ret = sFsShell.run("persist", filePath1, filePath2, filePath3);
    Assert.assertEquals(0, ret);
    checkFilePersisted(new AlluxioURI(filePath1), 10);
    checkFilePersisted(new AlluxioURI(filePath2), 20);
    checkFilePersisted(new AlluxioURI(filePath3), 30);
  }

  /**
   * Tests persisting files and directories together in one persist command.
   */
  @Test
  public void persistMultiFilesAndDirs() throws Exception {
    ServerConfiguration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    assertFalse(sFileSystem.getStatus(new AlluxioURI(testDir)).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(testDir + "/foo")).isPersisted());
    assertFalse(
        sFileSystem.getStatus(new AlluxioURI(testDir + "/foo/foobar2")).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(testDir + "/bar")).isPersisted());

    int ret = sFsShell.run("persist", testDir + "/foo/foobar1", testDir + "/foobar4",
        testDir + "/bar", testDir + "/bar/foobar3");
    Assert.assertEquals(0, ret);
    assertTrue(sFileSystem.getStatus(new AlluxioURI(testDir)).isPersisted());
    assertTrue(sFileSystem.getStatus(new AlluxioURI(testDir + "/foo")).isPersisted());
    assertFalse(
        sFileSystem.getStatus(new AlluxioURI(testDir + "/foo/foobar2")).isPersisted());
    assertTrue(sFileSystem.getStatus(new AlluxioURI(testDir + "/bar")).isPersisted());
    checkFilePersisted(new AlluxioURI(testDir + "/foo/foobar1"), 10);
    checkFilePersisted(new AlluxioURI(testDir + "/bar/foobar3"), 30);
    checkFilePersisted(new AlluxioURI(testDir + "/foobar4"), 40);
  }

  @Test
  public void persistNonexistentFile() throws Exception {
    // Cannot persist a nonexistent file
    String path = "/testPersistNonexistent";
    int ret = sFsShell.run("persist", path);
    Assert.assertEquals(-1, ret);
    Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path) + "\n",
        mOutput.toString());
  }

  @Test
  public void persistTwice() throws Exception {
    // Persisting an already-persisted file is okay
    String testFilePath = "/testPersist/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, testFilePath, WritePType.MUST_CACHE, 10);
    assertFalse(sFileSystem.getStatus(new AlluxioURI("/testPersist/testFile")).isPersisted());
    int ret = sFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    ret = sFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    checkFilePersisted(new AlluxioURI("/testPersist/testFile"), 10);
  }

  @Test
  public void persistWithAncestorPermission() throws Exception {
    String ufsRoot = sFileSystem.getStatus(new AlluxioURI("/")).getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
    // Skip non-local and non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(ufs) || UnderFileSystemUtils.isHdfs(ufs));

    AlluxioURI testFile = new AlluxioURI("/grand/parent/file");
    AlluxioURI grandParent = new AlluxioURI("/grand");
    Mode grandParentMode = new Mode((short) 0777);
    FileSystemTestUtils.createByteFile(sFileSystem, testFile, WritePType.MUST_CACHE, 10);
    URIStatus status = sFileSystem.getStatus(testFile);
    assertFalse(status.isPersisted());
    sFileSystem.setAttribute(grandParent,
        SetAttributePOptions.newBuilder().setMode(grandParentMode.toProto()).build());
    int ret = sFsShell.run("persist", testFile.toString());

    Assert.assertEquals(0, ret);
    checkFilePersisted(testFile, 10);

    // Check the permission of the created file and ancestor dir are in-sync between Alluxio and UFS
    short fileMode = (short) status.getMode();
    short parentMode = (short) sFileSystem.getStatus(testFile.getParent()).getMode();
    Assert.assertEquals(fileMode,
        ufs.getFileStatus(PathUtils.concatPath(ufsRoot, testFile)).getMode());
    Assert.assertEquals(parentMode,
        ufs.getDirectoryStatus(PathUtils.concatPath(ufsRoot, testFile.getParent())).getMode());
    Assert.assertEquals(grandParentMode,
        new Mode(ufs.getDirectoryStatus(PathUtils.concatPath(ufsRoot, grandParent)).getMode()));
  }

  @Test
  public void persistWithWildCard() throws Exception {
    String filePath1 = "/testPersist1/testFile1";
    String filePath2 = "/testPersist2/testFile2";
    String filePath3 = "/testPersist2/testFile3";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath1, WritePType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, filePath2, WritePType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(sFileSystem, filePath3, WritePType.MUST_CACHE, 30);

    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath1)).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath2)).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath3)).isPersisted());

    int ret = sFsShell.run("persist", "/*/testFile*");
    Assert.assertEquals(0, ret);
    checkFilePersisted(new AlluxioURI(filePath1), 10);
    checkFilePersisted(new AlluxioURI(filePath2), 20);
    checkFilePersisted(new AlluxioURI(filePath3), 30);
  }

  @Test
  public void persistRecursive() throws Exception {
    String filePath1 = "/testPersistRecursive/testFile1";
    String filePath2 = "/testPersistRecursive/testDir1/testFile2";
    String filePath3 = "/testPersistRecursive/testDir1/testDir2/testFile3";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath1, WritePType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, filePath2, WritePType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(sFileSystem, filePath3, WritePType.MUST_CACHE, 30);

    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath1)).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath2)).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath3)).isPersisted());

    int ret = sFsShell.run("persist", "/testPersistRecursive");
    Assert.assertEquals(0, ret);
    checkFilePersisted(new AlluxioURI(filePath1), 10);
    checkFilePersisted(new AlluxioURI(filePath2), 20);
    checkFilePersisted(new AlluxioURI(filePath3), 30);
  }

  @Test
  public void persistPartial() throws Exception {
    String filePath1 = "/testPersistPartial/testFile1";
    String filePath2 = "/testPersistPartial/testFile2";
    String filePath3 = "/testPersistPartial/testFile3";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath1, WritePType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, filePath2, WritePType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(sFileSystem, filePath3, WritePType.MUST_CACHE, 30);

    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath1)).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath2)).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath3)).isPersisted());

    // Persist testFile1 first.
    int ret = sFsShell.run("persist", "/testPersistPartial/testFile1");
    Assert.assertEquals(0, ret);
    checkFilePersisted(new AlluxioURI(filePath1), 10);
    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath2)).isPersisted());
    assertFalse(sFileSystem.getStatus(new AlluxioURI(filePath3)).isPersisted());

    // Persist entire directory.
    ret = sFsShell.run("persist", "/testPersistPartial");
    Assert.assertEquals(0, ret);
    checkFilePersisted(new AlluxioURI(filePath1), 10);
    checkFilePersisted(new AlluxioURI(filePath2), 20);
    checkFilePersisted(new AlluxioURI(filePath3), 30);
  }

  @Test
  public void persistParallelFilesEqParallelism() throws Exception {
    persistParallel(10, 10);
  }

  @Test
  public void persistParallelFilesLtParallelism() throws Exception {
    persistParallel(10, 100);
  }

  @Test
  public void persistParallelFilesGtParallelism() throws Exception {
    persistParallel(2, 1);
  }

  @Test
  public void persistWithWaitTimeTest() throws Exception {
    String filePath = "/testPersistWaitTime/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("persist", "--wait", "1s", "/*");
    Assert.assertEquals(0, ret);
    checkFilePersisted(new AlluxioURI(filePath), 10);
  }

  @Test
  public void persistWithWaitTimeBiggerThanTimeoutTest() throws Exception {
    String filePath = "/testPersistWaitTimeValid/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, filePath, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("persist", "--wait", "2s", "--timeout", "1s", "/*");
    Assert.assertEquals(-1, ret);
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {
          PropertyKey.Name.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS, "1s",
          PropertyKey.Name.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "1s",
          PropertyKey.Name.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "1s"})
  public void persistShortTimeout() throws Exception {
    shortTimeoutTest("--timeout");
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {
          PropertyKey.Name.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS, "1s",
          PropertyKey.Name.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "1s",
          PropertyKey.Name.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "1s"})
  public void persistShortTimeoutShortOption() throws Exception {
    shortTimeoutTest("-t");
  }

  /**
   * Ensure a short timeout results in a proper log message.
   *
   * @param option the short or long-form command line option
   */
  void shortTimeoutTest(String option) throws Exception {
    createUnpersistedFiles("/testPersistTimeout", 100, 2);
    sFsShell.run("persist", option, String.valueOf(1), "/*"); // 1ms persist timeout
    assertTrue("Should log at least one timeout",
        mLogRule.wasLogged("Timed out waiting for file to be persisted:"));
  }

  @Test
  public void persistLongTimeout() throws Exception {
    int fileSize = 100;
    List<AlluxioURI> files = createUnpersistedFiles("/testPersistTimeout", fileSize, 2);
    int ret = sFsShell.run("persist", "--timeout", String.valueOf(60 * 1000), "/*");
    assertEquals("shell should not report error", 0, ret);
    assertFalse("Should not have logged timeout",
        mLogRule.wasLogged("Timed out waiting for file to be persisted:"));
    for (AlluxioURI file : files) {
      checkFilePersisted(file, fileSize);
    }
  }

  private List<AlluxioURI> createUnpersistedFiles(String dirPath, int fileSize, int totalFiles)
      throws Exception {
    List<AlluxioURI> files = new ArrayList<>(totalFiles);
    for (int i = 0; i < totalFiles; i++) {
      String path = String.format("%s/%d", dirPath, i);
      files.add(new AlluxioURI(path));
      FileSystemTestUtils.createByteFile(sFileSystem, path, WritePType.MUST_CACHE, fileSize);
      assertFalse(sFileSystem.getStatus(files.get(i)).isPersisted());
    }
    return files;
  }

  public void persistParallel(int totalFiles, int parallelism) throws Exception {
    int fileSize = 30;
    List<AlluxioURI> files = createUnpersistedFiles("/testPersistParallel", fileSize, totalFiles);
    int ret = sFsShell.run("persist", "--parallelism", String.valueOf(parallelism), "/*");
    Assert.assertEquals(0, ret);
    for (AlluxioURI file : files) {
      checkFilePersisted(file, fileSize);
    }
  }
}
