/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

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
