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
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for load command.
 */
public class LoadCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void loadDirTest() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.THROUGH, 10);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testFileB", WriteType.MUST_CACHE, 10);
    AlluxioURI uriA = new AlluxioURI("/testRoot/testFileA");
    AlluxioURI uriB = new AlluxioURI("/testRoot/testFileB");

    URIStatus statusA = mFileSystem.getStatus(uriA);
    URIStatus statusB = mFileSystem.getStatus(uriB);
    Assert.assertFalse(statusA.getInMemoryPercentage() == 100);
    Assert.assertTrue(statusB.getInMemoryPercentage() == 100);
    // Testing loading of a directory
    mFsShell.run("load", "/testRoot");
    statusA = mFileSystem.getStatus(uriA);
    statusB = mFileSystem.getStatus(uriB);
    Assert.assertTrue(statusA.getInMemoryPercentage() == 100);
    Assert.assertTrue(statusB.getInMemoryPercentage() == 100);
  }

  @Test
  public void loadFileTest() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.THROUGH, 10);
    AlluxioURI uri = new AlluxioURI("/testFile");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertFalse(status.getInMemoryPercentage() == 100);
    // Testing loading of a single file
    mFsShell.run("load", "/testFile");
    status = mFileSystem.getStatus(uri);
    Assert.assertTrue(status.getInMemoryPercentage() == 100);
  }
}
