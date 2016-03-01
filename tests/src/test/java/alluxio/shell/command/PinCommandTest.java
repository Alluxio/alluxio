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
import alluxio.Constants;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests the "pin" and "unpin" commands.
 */
public class PinCommandTest extends AbstractAlluxioShellTest {
  /**
   * Tests the "pin" and "unpin" commands. Creates a file and tests if unpinning it , then pinning
   * it and finally unpinning
   */
  @Test
  public void setIsPinnedTest() throws Exception {
    AlluxioURI filePath = new AlluxioURI("/testFile");
    FileSystemTestUtils.createByteFile(mFileSystem, filePath, WriteType.MUST_CACHE, 1);

    // Ensure that the file exists
    Assert.assertTrue(fileExist(filePath));

    // Unpin an unpinned file
    Assert.assertEquals(0, mFsShell.run("unpin", filePath.toString()));
    Assert.assertFalse(mFileSystem.getStatus(filePath).isPinned());

    // Pin the file
    Assert.assertEquals(0, mFsShell.run("pin", filePath.toString()));
    Assert.assertTrue(mFileSystem.getStatus(filePath).isPinned());

    // Unpin the file
    Assert.assertEquals(0, mFsShell.run("unpin", filePath.toString()));
    Assert.assertFalse(mFileSystem.getStatus(filePath).isPinned());
  }

  /**
   * Tests pinned files are not evicted when Alluxio reaches memory limit. This use cases
   * creates three files of size 5MB for each to be added to Alluxio. The first file is pinned
   * and is not expected to be evicted.Alluxio capacity is 10MB, hence once the third file is
   * added, Alluxio is forced to evict one file. Since the first file is pinned it will not be
   * evicted only the second file will be evicted
   */
  @Test
  @Ignore("ALLUXIO-1729")
  public void setPinTest() throws Exception {
    AlluxioURI filePathA = new AlluxioURI("/testFileA");
    AlluxioURI filePathB = new AlluxioURI("/testFileB");
    AlluxioURI filePathC = new AlluxioURI("/testFileC");
    int fileSize = 5 * Constants.MB;

    FileSystemTestUtils.createByteFile(mFileSystem, filePathA, WriteType.MUST_CACHE, fileSize);
    Assert.assertTrue(fileExist(filePathA));
    Assert.assertEquals(0, mFsShell.run("pin", filePathA.toString()));

    FileSystemTestUtils.createByteFile(mFileSystem, filePathB, WriteType.MUST_CACHE, fileSize);
    Assert.assertTrue(fileExist(filePathB));
    Assert.assertEquals(0, mFsShell.run("unpin", filePathB.toString()));

    FileSystemTestUtils.createByteFile(mFileSystem, filePathC, WriteType.MUST_CACHE, fileSize);
    Assert.assertTrue(fileExist(new AlluxioURI(filePathC.toString())));

    // fileA is in memory because it is pinned, but not fileB
    Assert.assertEquals(100, mFileSystem.getStatus(filePathA).getInMemoryPercentage());
    Assert.assertEquals(0, mFileSystem.getStatus(filePathB).getInMemoryPercentage());
  }
}
