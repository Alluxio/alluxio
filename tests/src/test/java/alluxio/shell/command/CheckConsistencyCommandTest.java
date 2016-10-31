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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.underfs.UnderFileSystem;
import org.junit.Assert;
import org.junit.Test;

public class CheckConsistencyCommandTest extends AbstractAlluxioShellTest {
  /**
   * Tests the check consistency shell command correctly identifies a consistent subtree.
   */
  @Test
  public void consistent() throws Exception {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.CACHE_THROUGH, 10);
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testDir/testFileB",
        WriteType.CACHE_THROUGH, 20);
    mFsShell.run("checkConsistency", "/testRoot");
    String expected = "/testRoot is consistent with the under storage system.\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests the check consistency shell command correctly identifies inconsistent files in a subtree.
   */
  @Test
  public void inconsistent() throws Exception {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.CACHE_THROUGH, 10);
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testDir/testFileB",
        WriteType.CACHE_THROUGH, 20);
    String ufsPath = mFileSystem.getStatus(new AlluxioURI("/testRoot/testDir")).getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.get(ufsPath);
    ufs.delete(ufsPath, true);
    mFsShell.run("checkConsistency", "/testRoot");
    StringBuilder expected = new StringBuilder();
    expected.append("The following files are inconsistent:\n");
    expected.append("/testRoot/testDir\n");
    expected.append("/testRoot/testDir/testFileB\n");
    Assert.assertEquals(expected.toString(), mOutput.toString());
  }
}
