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
public class CopyToLocalCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void copyToLocalDirTest() throws IOException, AlluxioException {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret =
        mFsShell.run("copyToLocal", "/testWildCards/", mLocalAlluxioCluster.getAlluxioHome()
            + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foo/foobar1", 10);
    fileReadTest("/testDir/foo/foobar2", 20);
    fileReadTest("/testDir/bar/foobar3", 30);
    fileReadTest("/testDir/foobar4", 40);
  }

  @Test
  public void copyToLocalLargeTest() throws IOException {
    copyToLocalWithBytes(SIZE_BYTES);
  }

  @Test
  public void copyToLocalTest() throws IOException {
    copyToLocalWithBytes(10);
  }

  @Test
  public void copyToLocalWildcardExistingDirTest() throws IOException, AlluxioException {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);

    new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir").mkdir();

    int ret =
        mFsShell.run("copyToLocal", "/testWildCards/*/foo*", mLocalAlluxioCluster.getAlluxioHome()
            + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }

  @Test
  public void copyToLocalWildcardHierTest() throws IOException, AlluxioException {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret =
        mFsShell.run("copyToLocal", "/testWildCards/*", mLocalAlluxioCluster.getAlluxioHome()
            + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foo/foobar1", 10);
    fileReadTest("/testDir/foo/foobar2", 20);
    fileReadTest("/testDir/bar/foobar3", 30);
    fileReadTest("/testDir/foobar4", 40);
  }

  @Test
  public void copyToLocalWildcardNotDirTest() throws IOException, AlluxioException {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir").mkdir();
    new File(mLocalAlluxioCluster.getAlluxioHome() + "/testDir/testFile").createNewFile();

    int ret =
        mFsShell.run("copyToLocal", "/testWildCards/*/foo*", mLocalAlluxioCluster.getAlluxioHome()
            + "/testDir/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void copyToLocalWildcardTest() throws IOException, AlluxioException {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    int ret =
        mFsShell.run("copyToLocal", "/testWildCards/*/foo*", mLocalAlluxioCluster.getAlluxioHome()
            + "/testDir");
    Assert.assertEquals(0, ret);
    fileReadTest("/testDir/foobar1", 10);
    fileReadTest("/testDir/foobar2", 20);
    fileReadTest("/testDir/foobar3", 30);
  }
}
