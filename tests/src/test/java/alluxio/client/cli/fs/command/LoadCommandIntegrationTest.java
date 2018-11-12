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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;

import alluxio.grpc.WritePType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for load command.
 */
public final class LoadCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void loadDir() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testFileA", WritePType.WRITE_THROUGH,
        10);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testFileB", WritePType.WRITE_MUST_CACHE, 10);
    AlluxioURI uriA = new AlluxioURI("/testRoot/testFileA");
    AlluxioURI uriB = new AlluxioURI("/testRoot/testFileB");

    URIStatus statusA = mFileSystem.getStatus(uriA);
    URIStatus statusB = mFileSystem.getStatus(uriB);
    assertFalse(statusA.getInAlluxioPercentage() == 100);
    assertTrue(statusB.getInAlluxioPercentage() == 100);
    // Testing loading of a directory
    mFsShell.run("load", "/testRoot");
    statusA = mFileSystem.getStatus(uriA);
    statusB = mFileSystem.getStatus(uriB);
    assertTrue(statusA.getInAlluxioPercentage() == 100);
    assertTrue(statusB.getInAlluxioPercentage() == 100);
  }

  @Test
  public void loadFile() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WritePType.WRITE_THROUGH, 10);
    AlluxioURI uri = new AlluxioURI("/testFile");
    URIStatus status = mFileSystem.getStatus(uri);
    assertFalse(status.getInAlluxioPercentage() == 100);
    // Testing loading of a single file
    mFsShell.run("load", "/testFile");
    status = mFileSystem.getStatus(uri);
    assertTrue(status.getInAlluxioPercentage() == 100);
  }

  @Test
  public void loadFileWithLocalOption() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WritePType.WRITE_CACHE_THROUGH,
        10);
    AlluxioURI uri = new AlluxioURI("/testFile");
    URIStatus status = mFileSystem.getStatus(uri);
    assertTrue(status.getInAlluxioPercentage() == 100);
    // Testing loading a file has been loaded fully
    mFsShell.run("load", "--local", "/testFile");
    Assert.assertEquals("/testFile" + " loaded" + "\n", mOutput.toString());
    // Testing "load --local" works when the file isn't already loaded
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile2", WritePType.WRITE_THROUGH, 10);
    uri = new AlluxioURI("/testFile2");
    status = mFileSystem.getStatus(uri);
    assertFalse(status.getInAlluxioPercentage() == 100);
    mFsShell.run("load", "--local", "/testFile2");
    status = mFileSystem.getStatus(uri);
    assertTrue(status.getInAlluxioPercentage() == 100);
  }

  @Test
  public void loadFileWithWildcard() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testDir1/testFile1", WritePType.WRITE_THROUGH,
        10);
    FileSystemTestUtils.createByteFile(mFileSystem, "/testDir2/testFile2", WritePType.WRITE_THROUGH,
        10);
    AlluxioURI uri = new AlluxioURI("/testDir1/testFile1");
    URIStatus status = mFileSystem.getStatus(uri);
    assertFalse(status.getInAlluxioPercentage() == 100);
    uri = new AlluxioURI("/testDir2/testFile2");
    status = mFileSystem.getStatus(uri);
    assertFalse(status.getInAlluxioPercentage() == 100);

    // Testing loading with wild card
    mFsShell.run("load", "/*/testFile*");
    uri = new AlluxioURI("/testDir1/testFile1");
    status = mFileSystem.getStatus(uri);
    assertTrue(status.getInAlluxioPercentage() == 100);
    uri = new AlluxioURI("/testDir2/testFile2");
    status = mFileSystem.getStatus(uri);
    assertTrue(status.getInAlluxioPercentage() == 100);
  }
}
