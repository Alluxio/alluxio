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
import alluxio.exception.ExceptionMessage;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.grpc.WritePType;
import alluxio.util.io.BufferUtils;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests checksum command.
 */
public final class ChecksumCommandIntegrationTest extends AbstractFileSystemShellTest {

  /**
   * Tests md5 checksum calculation.
   */
  @Test
  public void checksum() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    sFsShell.run("checksum", "/testFile");
    String str = mOutput.toString();
    String[] splitString = str.split("\\s+");

    byte[] data = BufferUtils.getIncreasingByteArray(10);
    try {
      String expectedMd5 = DigestUtils.md5Hex(data);
      Assert.assertEquals(splitString[1], expectedMd5);
    } catch (Exception e) {
      Assert.fail("md5cksum failure not expected: " + e.getMessage());
    }
  }

  /**
   * Tests md5 checksum calculation with wildcard.
   */
  @Test
  public void checksumWildCard() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testDir/testFileA",
        WritePType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testDir2/testFileB",
        WritePType.MUST_CACHE, 10);
    sFsShell.run("checksum", "/testDir*/testFile*");
    String str = mOutput.toString();
    String[] splitString = str.split("\\s+");

    byte[] data = BufferUtils.getIncreasingByteArray(10);
    try {
      String expectedMd5 = DigestUtils.md5Hex(data);
      Assert.assertEquals(splitString[1], expectedMd5);
      Assert.assertEquals(splitString[3], expectedMd5);
    } catch (Exception e) {
      Assert.fail("md5cksum failure not expected: " + e.getMessage());
    }
  }

  /**
   * Test invalid args.
   */
  @Test
  public void checksumInvalidArgs() throws Exception {
    sFsShell.run("checksum", "/testFile");
    String expected = ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/testFile") + "\n";
    Assert.assertEquals(expected, mOutput.toString());
    sFsShell.run("mkdir", "/testFolder");
    int ret = sFsShell.run("checksum", "/testFolder");
    Assert.assertEquals(-1, ret);
  }
}
