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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests consisting of multiple commands.
 */
public class ComplexCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void createCacheInsertInUfsThenloadMetadataTest() throws IOException, AlluxioException {
    // Construct a situation where the directory exists in the inode tree and the UFS, but is not
    // marked as persisted.
    FileSystemTestUtils.createByteFile(mFileSystem, "/testDir/testFileA", WriteType.MUST_CACHE, 10);
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI("/testDir")).isPersisted());
    Configuration conf = mLocalAlluxioCluster.getMasterConf();
    String ufsRoot = conf.get(Constants.UNDERFS_ADDRESS);
    UnderFileSystemUtils.mkdirIfNotExists(PathUtils.concatPath(ufsRoot, "testDir"), conf);
    Assert.assertFalse(mFileSystem.getStatus(new AlluxioURI("/testDir")).isPersisted());
    // Load metadata, which should mark the testDir as persisted
    mFsShell.run("loadMetadata", "/testDir");
    Assert.assertEquals("", mOutput.toString());
    Assert.assertTrue(mFileSystem.getStatus(new AlluxioURI("/testDir")).isPersisted());
  }

  @Test
  public void lsThenloadMetadataTest() throws IOException, AlluxioException {
    Configuration conf = mLocalAlluxioCluster.getMasterConf();
    String ufsRoot = conf.get(Constants.UNDERFS_ADDRESS);
    UnderFileSystemUtils.mkdirIfNotExists(PathUtils.concatPath(ufsRoot, "dir1"), conf);
    // First run ls to create the data
    mFsShell.run("ls", "/dir1");
    Assert.assertTrue(mFileSystem.getStatus(new AlluxioURI("/dir1")).isPersisted());
    // Load metadata
    mFsShell.run("loadMetadata", "/dir1");
    Assert.assertEquals(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage("/dir1") + "\n",
        mOutput.toString());
  }
}
