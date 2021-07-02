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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.grpc.WritePType;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for load command.
 */
public final class LoadCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void loadDir() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testFileA", WritePType.THROUGH,
        10);
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testRoot/testFileB", WritePType.MUST_CACHE, 10);
    AlluxioURI uriA = new AlluxioURI("/testRoot/testFileA");
    AlluxioURI uriB = new AlluxioURI("/testRoot/testFileB");

    URIStatus statusA = sFileSystem.getStatus(uriA);
    URIStatus statusB = sFileSystem.getStatus(uriB);
    assertFalse(statusA.getInAlluxioPercentage() == 100);
    assertTrue(statusB.getInAlluxioPercentage() == 100);
    // Testing loading of a directory
    sFsShell.run("load", "/testRoot");
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uriA, 100);
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uriB, 100);
  }

  @Test
  public void loadFile() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.THROUGH, 10);
    AlluxioURI uri = new AlluxioURI("/testFile");
    URIStatus status = sFileSystem.getStatus(uri);
    assertFalse(status.getInAlluxioPercentage() == 100);
    // Testing loading of a single file
    sFsShell.run("load", "/testFile");
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uri, 100);
  }

  @Test
  public void loadFileWithLocalOption() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.CACHE_THROUGH,
        10);
    AlluxioURI uri = new AlluxioURI("/testFile");
    URIStatus status = sFileSystem.getStatus(uri);
    assertTrue(status.getInAlluxioPercentage() == 100);
    // Testing loading a file has been loaded fully
    sFsShell.run("load", "--local", "/testFile");
    Assert.assertEquals("/testFile" + " loaded" + "\n", mOutput.toString());
    // Testing "load --local" works when the file isn't already loaded
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile2", WritePType.THROUGH, 10);
    uri = new AlluxioURI("/testFile2");
    status = sFileSystem.getStatus(uri);
    assertFalse(status.getInAlluxioPercentage() == 100);
    sFsShell.run("load", "--local", "/testFile2");
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uri, 100);
  }

  @Test
  public void loadFileWithWildcard() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testDir1/testFile1", WritePType.THROUGH,
        10);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testDir2/testFile2", WritePType.THROUGH,
        10);
    AlluxioURI uri = new AlluxioURI("/testDir1/testFile1");
    URIStatus status = sFileSystem.getStatus(uri);
    assertFalse(status.getInAlluxioPercentage() == 100);
    uri = new AlluxioURI("/testDir2/testFile2");
    status = sFileSystem.getStatus(uri);
    assertFalse(status.getInAlluxioPercentage() == 100);

    // Testing loading with wild card
    sFsShell.run("load", "/*/testFile*");
    uri = new AlluxioURI("/testDir1/testFile1");
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uri, 100);
    uri = new AlluxioURI("/testDir2/testFile2");
    FileSystemUtils.waitForAlluxioPercentage(sFileSystem, uri, 100);
  }
}
