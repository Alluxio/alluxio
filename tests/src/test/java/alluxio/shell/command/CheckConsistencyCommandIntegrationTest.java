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
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.options.ExistsOptions;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;
import alluxio.wire.LoadMetadataType;

import org.junit.Assert;
import org.junit.Test;

import java.io.OutputStream;

/**
 * Integration tests for checkConsistency command.
 */
public class CheckConsistencyCommandIntegrationTest extends AbstractAlluxioShellTest {
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
    mOutput.reset();
    mFsShell.run("checkConsistency", "-r", "/testRoot");
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
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath);
    ufs.deleteDirectory(ufsPath, DeleteOptions.defaults().setRecursive(true));
    mFsShell.run("checkConsistency", "/testRoot");
    StringBuilder expected = new StringBuilder();
    expected.append("The following files are inconsistent:\n");
    expected.append("/testRoot/testDir\n");
    expected.append("/testRoot/testDir/testFileB\n");
    Assert.assertEquals(expected.toString(), mOutput.toString());

    mOutput.reset();
    mFsShell.run("checkConsistency", "-r", "/testRoot");
    String res = mOutput.toString();
    Assert.assertTrue(res.contains("/testRoot" + " has: " + "2 inconsistent files.\n")
        && res.contains("repairing path: " + "/testRoot/testDir\n")
        && res.contains("repairing path: " + "/testRoot/testDir/testFileB\n"));
    Assert.assertTrue(!mFileSystem.exists(new AlluxioURI("/testRoot/testDir")));
    Assert.assertTrue(!mFileSystem.exists(new AlluxioURI("/testRoot/testDir/testFileB")));

    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testDir/testFileB",
        WriteType.CACHE_THROUGH, 20);
    ufsPath = mFileSystem.getStatus(new AlluxioURI("/testRoot/testDir/testFileB")).getUfsPath();
    ufs.deleteFile(ufsPath);
    OutputStream outputStream = ufs.create(ufsPath);
    byte[] bytes = {1, 2, 3};
    outputStream.write(bytes);
    outputStream.close();
    mOutput.reset();
    mFsShell.run("checkConsistency", "-r", "/testRoot");
    res = mOutput.toString();
    Assert.assertTrue(res.contains("/testRoot" + " has: " + "1 inconsistent files.\n")
        && res.contains("repairing path: " + "/testRoot/testDir/testFileB\n"));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/testRoot/testDir/testFileB"),
        ExistsOptions.defaults().setLoadMetadataType(LoadMetadataType.Always)));
    Assert.assertTrue(3 == mFileSystem.getStatus(new AlluxioURI("/testRoot/testDir/testFileB"))
        .getLength());
  }
}
