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

import alluxio.client.WriteType;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for head command.
 */
public final class HeadCommandIntegrationTest extends AbstractAlluxioShellTest {

  @Test
  public void headEmptyFile() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/emptyFile", WriteType.MUST_CACHE, 0);
    int ret = mFsShell.run("head", "/emptyFile");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void headLargeFile() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 2048);
    mFsShell.run("head", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(1024, 1024);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void headNotExit() throws Exception {
    int ret = mFsShell.run("head", "/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void headSmallFile() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("head", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void headWildcard() throws Exception {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);

    // the expect contents (remember that the order is based on the path)
    byte[] exp1 = BufferUtils.getIncreasingByteArray(30); // testDir/bar/foobar3
    byte[] exp2 = BufferUtils.getIncreasingByteArray(10); // testDir/foo/foobar1
    byte[] exp3 = BufferUtils.getIncreasingByteArray(20); // testDir/foo/foobar2
    byte[] expect = new byte[exp1.length + exp2.length + exp3.length];
    System.arraycopy(exp1, 0, expect, 0, exp1.length);
    System.arraycopy(exp2, 0, expect, exp1.length, exp2.length);
    System.arraycopy(exp3, 0, expect, exp1.length + exp2.length, exp3.length);

    int ret = mFsShell.run("head", testDir + "/*/foo*");
    Assert.assertEquals(0, ret);
    Assert.assertArrayEquals(mOutput.toByteArray(), expect);
  }

  @Test
  public void headFileWithUserSpecifiedBytes() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10000);
    mFsShell.run("head", "-c", "10000", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(0, 10000);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }
}
