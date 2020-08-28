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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.grpc.WritePType;

import org.junit.Test;

/**
 * Tests the "pin" and "unpin" commands.
 */
public final class PinCommandIntegrationTest extends AbstractFileSystemShellTest {
  /**
   * Tests the "pin" and "unpin" commands. Creates a file and tests unpinning it, then pinning
   * it and finally unpinning
   */
  @Test
  public void setIsPinned() throws Exception {
    AlluxioURI filePath = new AlluxioURI("/testFile");
    FileSystemTestUtils.createByteFile(sFileSystem, filePath, WritePType.MUST_CACHE, 1);

    // Ensure that the file exists
    assertTrue(fileExists(filePath));

    // Unpin an unpinned file
    assertEquals(0, sFsShell.run("unpin", filePath.toString()));
    assertFalse(sFileSystem.getStatus(filePath).isPinned());

    // Pin the file
    assertEquals(0, sFsShell.run("pin", filePath.toString()));
    assertTrue(sFileSystem.getStatus(filePath).isPinned());

    // Unpin the file
    assertEquals(0, sFsShell.run("unpin", filePath.toString()));
    assertFalse(sFileSystem.getStatus(filePath).isPinned());
  }

  /**
   * Tests pinned files are not evicted when Alluxio reaches memory limit. This test case creates
   * three files, each file is half the size of the cluster's capacity. The three files are added
   * sequentially to the cluster, the first file is pinned. When the third file is added, the two
   * previous files have already occupied the whole capacity, so one file needs to be evicted to
   * spare space for the third file. Since the first file is pinned, it will not be evicted, so only
   * the second file will be evicted.
   */
  @Test
  public void setPin() throws Exception {
    AlluxioURI filePathA = new AlluxioURI("/testFileA");
    AlluxioURI filePathB = new AlluxioURI("/testFileB");
    AlluxioURI filePathC = new AlluxioURI("/testFileC");
    int fileSize = SIZE_BYTES / 2;

    FileSystemTestUtils.createByteFile(sFileSystem, filePathA, WritePType.MUST_CACHE,
        fileSize);
    assertTrue(fileExists(filePathA));
    assertEquals(0, sFsShell.run("pin", filePathA.toString()));

    FileSystemTestUtils.createByteFile(sFileSystem, filePathB, WritePType.MUST_CACHE,
        fileSize);
    assertTrue(fileExists(filePathB));
    assertEquals(0, sFsShell.run("unpin", filePathB.toString()));

    FileSystemTestUtils.createByteFile(sFileSystem, filePathC, WritePType.MUST_CACHE,
        fileSize);
    assertTrue(fileExists(filePathC));

    // fileA is in memory because it is pinned, but fileB should have been evicted to hold fileC.
    assertEquals(100, sFileSystem.getStatus(filePathA).getInAlluxioPercentage());
    assertEquals(0, sFileSystem.getStatus(filePathB).getInAlluxioPercentage());
    // fileC should be in memory because fileB is evicted.
    assertEquals(100, sFileSystem.getStatus(filePathC).getInAlluxioPercentage());
  }
}
