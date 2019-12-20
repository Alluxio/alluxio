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

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.grpc.WritePType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for chmod command.
 */
public final class ChmodCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void chmod() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    sFsShell.run("chmod", "777", "/testFile");
    int permission = sFileSystem.getStatus(new AlluxioURI("/testFile")).getMode();
    Assert.assertEquals((short) 0777, permission);
    sFsShell.run("chmod", "755", "/testFile");
    permission = sFileSystem.getStatus(new AlluxioURI("/testFile")).getMode();
    Assert.assertEquals((short) 0755, permission);
  }

  /**
   * Tests -R option for chmod recursively.
   */
  @Test
  public void chmodRecursively() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testDir/testFile",
        WritePType.MUST_CACHE, 10);
    sFsShell.run("chmod", "-R", "777", "/testDir");
    int permission = sFileSystem.getStatus(new AlluxioURI("/testDir")).getMode();
    Assert.assertEquals((short) 0777, permission);
    permission = sFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getMode();
    Assert.assertEquals((short) 0777, permission);
    sFsShell.run("chmod", "-R", "755", "/testDir");
    permission = sFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getMode();
    Assert.assertEquals((short) 0755, permission);
  }

  @Test
  public void chmodSymbolic() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    sFsShell.run("chmod", "a=rwx", "/testFile");
    int permission = sFileSystem.getStatus(new AlluxioURI("/testFile")).getMode();
    Assert.assertEquals((short) 0777, permission);
    sFsShell.run("chmod", "u=rwx,go=rx", "/testFile");
    permission = sFileSystem.getStatus(new AlluxioURI("/testFile")).getMode();
    Assert.assertEquals((short) 0755, permission);
  }

  /**
   * Tests wildcard entries for chmod.
   */
  @Test
  public void chmodWildCard() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testDir/testFile1",
        WritePType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testDir2/testFile2",
        WritePType.MUST_CACHE, 10);
    sFsShell.run("chmod", "a=rwx", "/testDir*/testFile*");
    int permission = sFileSystem.getStatus(new AlluxioURI("/testDir/testFile1")).getMode();
    Assert.assertEquals((short) 0777, permission);
    permission = sFileSystem.getStatus(new AlluxioURI("/testDir2/testFile2")).getMode();
    Assert.assertEquals((short) 0777, permission);
    sFsShell.run("chmod", "u=rwx,go=rx", "/testDir*/testFile*");
    permission = sFileSystem.getStatus(new AlluxioURI("/testDir/testFile1")).getMode();
    Assert.assertEquals((short) 0755, permission);
    permission = sFileSystem.getStatus(new AlluxioURI("/testDir2/testFile2")).getMode();
    Assert.assertEquals((short) 0755, permission);
  }
}
