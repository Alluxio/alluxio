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

import org.junit.Assert;
import org.junit.Test;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.shell.AbstractAlluxioShellTest;

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
   * Tests pinned files are not evicted when Tachyon reaches memory limit. This use cases
   * creates three files of size 5MB for each to be added to Tachyon. The first file is pinned
   * and is not expected to be evicted.Tachyon capacity is 10MB, hence once the third file is
   * added, Tachyon is forced to evict one file. Since the first file is pinned it will not be
   * evicted only the second file will be evicted
   */
  @Test
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
