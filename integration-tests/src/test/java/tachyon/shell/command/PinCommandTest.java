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
import org.junit.Ignore;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.TachyonFile;
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
    String filePath = "/testFile";
    TachyonFile file = TachyonFSTestUtils.createByteFile(mTfs, filePath, TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, 1);

    // Ensure that the file exists
    Assert.assertTrue(fileExist(new TachyonURI(filePath)));

    // Unpin an unpinned file
    Assert.assertEquals(0, mFsShell.run("unpin", filePath));
    Assert.assertFalse(mTfs.getInfo(file).isPinned);

    // Pin the file
    Assert.assertEquals(0, mFsShell.run("pin", filePath));
    Assert.assertTrue(mTfs.getInfo(file).isPinned);

    // Unpin the file
    Assert.assertEquals(0, mFsShell.run("unpin", filePath));
    Assert.assertFalse(mTfs.getInfo(file).isPinned);
  }

  /**
   * Tests pinned files are not evicted when Tachyon reaches memory limit. This use cases
   * creates three files of size 5MB for each to be added to Tachyon. The first file is pinned
   * and is not expected to be evected.Tachyon capacity is 10MB, hence once the third file is
   * added, Tachyon is forced to evict one file. Since the first file is pinned it will not be
   * evicted only the second file will be evicted
   */
  @Test
  @Ignore("See TACHYON-1579")
  public void setPinTest() throws Exception {
    String filePathA = "/testFileA";
    String filePathB = "/testFileB";
    String filePathC = "/testFileC";
    int fileSize = 5 * Constants.MB;

    TachyonFile fileA = TachyonFSTestUtils.createByteFile(mTfs, filePathA, TachyonStorageType.STORE,
         UnderStorageType.NO_PERSIST, fileSize);
    Assert.assertTrue(fileExist(new TachyonURI(filePathA)));
    Assert.assertEquals(0, mFsShell.run("pin", filePathA));

    TachyonFile fileB = TachyonFSTestUtils.createByteFile(mTfs, filePathB, TachyonStorageType.STORE,
         UnderStorageType.NO_PERSIST, fileSize);
    Assert.assertTrue(fileExist(new TachyonURI(filePathB)));
    Assert.assertEquals(0, mFsShell.run("unpin", filePathB));

    TachyonFile fileC = TachyonFSTestUtils.createByteFile(mTfs, filePathC, TachyonStorageType.STORE,
         UnderStorageType.NO_PERSIST, fileSize);
    Assert.assertTrue(fileExist(new TachyonURI(filePathC)));

    // fileA is in memory because it is pinned, but not fileB
    Assert.assertEquals(100, mTfs.getInfo(fileA).inMemoryPercentage);
    Assert.assertEquals(0, mTfs.getInfo(fileB).inMemoryPercentage);
  }
}
