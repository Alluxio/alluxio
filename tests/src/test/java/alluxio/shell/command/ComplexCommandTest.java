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

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.Configuration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

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
