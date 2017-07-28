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

import alluxio.SystemPropertyRule;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.util.HashMap;

/**
 * Tests for copyToLocal command.
 */
public final class CopyToLocalCommandIntegrationTest extends AbstractAlluxioShellTest {
  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Test
  public void copyToLocalDir() throws Exception {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret =
        mFsShell.run("copyToLocal", testDir, mLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foo/foobar1", 10);
    fileReadTest("/testDir/foo/foobar2", 20);
    fileReadTest("/testDir/bar/foobar3", 30);
    fileReadTest("/testDir/foobar4", 40);
  }

  @Test
  public void copyToLocalRelativePathDir() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put("user.dir", mTestFolder.getRoot().getAbsolutePath());
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      File localDir = mTestFolder.newFolder("localDir");
      localDir.mkdir();
      mFsShell.run("copyToLocal", "/testFile", "localDir");
      Assert.assertEquals("Copied /testFile to file://" + mTestFolder.getRoot().getAbsolutePath()
              + "/localDir/testFile" + "\n", mOutput.toString());
    }
  }

  @Test
  public void copyToLocalLarge() throws Exception {
    copyToLocalWithBytes(SIZE_BYTES);
  }

  @Test
  public void copyToLocal() throws Exception {
    copyToLocalWithBytes(10);
  }

  @Test
  public void copyToLocalWildcardExistingDir() throws Exception {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);

    new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir").mkdir();

    int ret = mFsShell.run("copyToLocal", testDir + "/*/foo*",
        mLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }

  @Test
  public void copyToLocalWildcardHier() throws Exception {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell
        .run("copyToLocal", testDir + "/*", mLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foo/foobar1", 10);
    fileReadTest("/testDir/foo/foobar2", 20);
    fileReadTest("/testDir/bar/foobar3", 30);
    fileReadTest("/testDir/foobar4", 40);
  }

  @Test
  public void copyToLocalWildcardNotDir() throws Exception {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir").mkdir();
    new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir/testFile").createNewFile();

    int ret = mFsShell.run("copyToLocal", testDir + "/*/foo*",
        mLocalAlluxioCluster.getAlluxioHome() + "/testDir/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyToLocalWildcard() throws Exception {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("copyToLocal", testDir + "/*/foo*",
        mLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }

  @Test
  public void copyToLocalRelativePath() throws Exception {
    HashMap<String, String> sysProps = new HashMap<>();
    sysProps.put("user.dir", mTestFolder.getRoot().getAbsolutePath());
    try (Closeable p = new SystemPropertyRule(sysProps).toResource()) {
      FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
      mFsShell.run("copyToLocal", "/testFile", ".");
      Assert.assertEquals("Copied /testFile to file://" + mTestFolder.getRoot().getAbsolutePath()
              + "/testFile" + "\n", mOutput.toString());
      mOutput.reset();
      mFsShell.run("copyToLocal", "/testFile", "./testFile");
      Assert.assertEquals("Copied /testFile to file://" + mTestFolder.getRoot().getAbsolutePath()
              + "/testFile" + "\n", mOutput.toString());
    }
  }
}
