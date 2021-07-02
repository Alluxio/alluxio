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

import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.grpc.WritePType;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for tail command.
 */
public final class TailCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void tailEmptyFile() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/emptyFile", WritePType.MUST_CACHE, 0);
    int ret = sFsShell.run("tail", "/emptyFile");
    Assert.assertEquals(0, ret);
  }

  @Test
  public void tailLargeFile() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 2048);
    sFsShell.run("tail", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(1024, 1024);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void tailNotExist() throws IOException {
    int ret = sFsShell.run("tail", "/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void tailSmallFile() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    sFsShell.run("tail", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void tailWildcard() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);

    // the expect contents (remember that the order is based on the path)
    byte[] exp1 = BufferUtils.getIncreasingByteArray(30); // testDir/bar/foobar3
    byte[] exp2 = BufferUtils.getIncreasingByteArray(10); // testDir/foo/foobar1
    byte[] exp3 = BufferUtils.getIncreasingByteArray(20); // testDir/foo/foobar2
    byte[] expect = new byte[exp1.length + exp2.length + exp3.length];
    System.arraycopy(exp1, 0, expect, 0, exp1.length);
    System.arraycopy(exp2, 0, expect, exp1.length, exp2.length);
    System.arraycopy(exp3, 0, expect, exp1.length + exp2.length, exp3.length);

    int ret = sFsShell.run("tail", testDir + "/*/foo*");
    Assert.assertEquals(0, ret);
    Assert.assertArrayEquals(mOutput.toByteArray(), expect);
  }

  @Test
  public void tailFileWithUserSpecifiedBytes() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 2048);
    sFsShell.run("tail", "-c", "2000", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(48, 2000);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void tailFileWithUserSpecifiedBytesWithUnit() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE,
        10000);
    sFsShell.run("tail", "-c", "2KB", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(10000 - 2048, 2048);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }
}
