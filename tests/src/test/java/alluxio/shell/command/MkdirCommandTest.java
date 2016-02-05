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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;

/**
 * Tests for mkdir command.
 */
public class MkdirCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void mkdirTest() throws IOException, AlluxioException {
    String qualifiedPath = "alluxio://" + mLocalAlluxioCluster.getMasterHostname() + ":"
        + mLocalAlluxioCluster.getMasterPort() + "/root/testFile1";
    mFsShell.run("mkdir", qualifiedPath);
    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/root/testFile1"));
    Assert.assertNotNull(status);
    Assert
        .assertEquals(getCommandOutput(new String[] {"mkdir", qualifiedPath}), mOutput.toString());
    Assert.assertTrue(status.isFolder());
  }

  @Test
  public void mkdirComplexPathTest() throws IOException, AlluxioException {
    mFsShell.run("mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File");
    URIStatus status =
        mFileSystem.getStatus(new AlluxioURI("/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"));
    Assert.assertNotNull(status);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir",
        "/Complex!@#$%^&*()-_=+[]{};\"'<>," + ".?/File"}), mOutput.toString());
    Assert.assertTrue(status.isFolder());
  }

  @Test
  public void mkdirExistingTest() throws IOException {
    Assert.assertEquals(0, mFsShell.run("mkdir", "/testFile1"));
    Assert.assertEquals(-1, mFsShell.run("mkdir", "/testFile1"));
  }

  @Test
  public void mkdirInvalidPathTest() throws IOException {
    Assert.assertEquals(-1, mFsShell.run("mkdir", "/test File Invalid Path"));
  }

  @Test
  public void mkdirMultiPathTest() throws IOException, AlluxioException {
    String path1 = "/testDir1";
    String path2 = "/testDir2";
    String path3 = "/testDir2/testDir2.1";
    Assert.assertEquals(0, mFsShell.run("mkdir", path1, path2, path3));

    URIStatus status = mFileSystem.getStatus(new AlluxioURI(path1));
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isFolder());

    status = mFileSystem.getStatus(new AlluxioURI(path2));
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isFolder());

    status = mFileSystem.getStatus(new AlluxioURI(path3));
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isFolder());

  }

  @Test
  public void mkdirShortPathTest() throws IOException, AlluxioException {
    mFsShell.run("mkdir", "/root/testFile1");
    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/root/testFile1"));
    Assert.assertNotNull(status);
    Assert.assertEquals(getCommandOutput(new String[]{"mkdir", "/root/testFile1"}),
        mOutput.toString());
    Assert.assertTrue(status.isFolder());
  }
}
