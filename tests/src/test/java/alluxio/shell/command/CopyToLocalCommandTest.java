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

import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Tests for copyToLocal command.
 */
public final class CopyToLocalCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void copyToLocalDir() throws IOException, AlluxioException {
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
  public void copyToLocalLarge() throws IOException {
    copyToLocalWithBytes(SIZE_BYTES);
  }

  @Test
  public void copyToLocal() throws IOException {
    copyToLocalWithBytes(10);
  }

  @Test
  public void copyToLocalWildcardExistingDir() throws IOException, AlluxioException {
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
  public void copyToLocalWildcardHier() throws IOException, AlluxioException {
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
  public void copyToLocalWildcardNotDir() throws IOException, AlluxioException {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir").mkdir();
    new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir/testFile").createNewFile();

    int ret = mFsShell.run("copyToLocal", testDir + "/*/foo*",
        mLocalAlluxioCluster.getAlluxioHome() + "/testDir/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyToLocalWildcard() throws IOException, AlluxioException {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret = mFsShell.run("copyToLocal", testDir + "/*/foo*",
        mLocalAlluxioCluster.getAlluxioHome() + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }
}
