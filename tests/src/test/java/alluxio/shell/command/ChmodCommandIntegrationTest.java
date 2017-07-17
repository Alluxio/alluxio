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
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for chmod command.
 */
public final class ChmodCommandIntegrationTest extends AbstractAlluxioShellTest {
  @Test
  public void chmod() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chmod", "777", "/testFile");
    int permission = mFileSystem.getStatus(new AlluxioURI("/testFile")).getMode();
    Assert.assertEquals((short) 0777, permission);
    mFsShell.run("chmod", "755", "/testFile");
    permission = mFileSystem.getStatus(new AlluxioURI("/testFile")).getMode();
    Assert.assertEquals((short) 0755, permission);
  }

  /**
   * Tests -R option for chmod recursively.
   */
  @Test
  public void chmodRecursively() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testDir/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chmod", "-R", "777", "/testDir");
    int permission = mFileSystem.getStatus(new AlluxioURI("/testDir")).getMode();
    Assert.assertEquals((short) 0777, permission);
    permission = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getMode();
    Assert.assertEquals((short) 0777, permission);
    mFsShell.run("chmod", "-R", "755", "/testDir");
    permission = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getMode();
    Assert.assertEquals((short) 0755, permission);
  }

  @Test
  public void chmodSymbolic() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chmod", "a=rwx", "/testFile");
    int permission = mFileSystem.getStatus(new AlluxioURI("/testFile")).getMode();
    Assert.assertEquals((short) 0777, permission);
    mFsShell.run("chmod", "u=rwx,go=rx", "/testFile");
    permission = mFileSystem.getStatus(new AlluxioURI("/testFile")).getMode();
    Assert.assertEquals((short) 0755, permission);
  }
}
