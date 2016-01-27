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

package tachyon.shell.command;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.FileSystemTestUtils;
import tachyon.client.WriteType;
import tachyon.shell.AbstractTfsShellTest;

/**
 * Tests the "pin" and "unpin" commands.
 */
public class PinCommandTest extends AbstractTfsShellTest {
  /**
   * Tests the "pin" and "unpin" commands. Creates a file and tests if unpinning it , then pinning
   * it and finally unpinning
   */
  @Test
  public void setIsPinnedTest() throws Exception {
    TachyonURI filePath = new TachyonURI("/testFile");
    FileSystemTestUtils.createByteFile(mTfs, filePath, WriteType.MUST_CACHE, 1);

    // Ensure that the file exists
    Assert.assertTrue(fileExist(filePath));

    // Unpin an unpinned file
    Assert.assertEquals(0, mFsShell.run("unpin", filePath.toString()));
    Assert.assertFalse(mTfs.getStatus(filePath).isPinned());

    // Pin the file
    Assert.assertEquals(0, mFsShell.run("pin", filePath.toString()));
    Assert.assertTrue(mTfs.getStatus(filePath).isPinned());

    // Unpin the file
    Assert.assertEquals(0, mFsShell.run("unpin", filePath.toString()));
    Assert.assertFalse(mTfs.getStatus(filePath).isPinned());
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
    TachyonURI filePathA = new TachyonURI("/testFileA");
    TachyonURI filePathB = new TachyonURI("/testFileB");
    TachyonURI filePathC = new TachyonURI("/testFileC");
    int fileSize = 5 * Constants.MB;

    FileSystemTestUtils.createByteFile(mTfs, filePathA, WriteType.MUST_CACHE, fileSize);
    Assert.assertTrue(fileExist(filePathA));
    Assert.assertEquals(0, mFsShell.run("pin", filePathA.toString()));

    FileSystemTestUtils.createByteFile(mTfs, filePathB, WriteType.MUST_CACHE, fileSize);
    Assert.assertTrue(fileExist(filePathB));
    Assert.assertEquals(0, mFsShell.run("unpin", filePathB.toString()));

    FileSystemTestUtils.createByteFile(mTfs, filePathC, WriteType.MUST_CACHE, fileSize);
    Assert.assertTrue(fileExist(new TachyonURI(filePathC.toString())));

    // fileA is in memory because it is pinned, but not fileB
    Assert.assertEquals(100, mTfs.getStatus(filePathA).getInMemoryPercentage());
    Assert.assertEquals(0, mTfs.getStatus(filePathB).getInMemoryPercentage());
  }
}
