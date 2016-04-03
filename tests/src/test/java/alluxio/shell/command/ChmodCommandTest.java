/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for chmod command.
 */
public class ChmodCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void chmodTest() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chmod", "777", "/testFile");
    int permission = mFileSystem.getStatus(new AlluxioURI("/testFile")).getPermission();
    Assert.assertEquals((short) 0777, permission);
    mFsShell.run("chmod", "755", "/testFile");
    permission = mFileSystem.getStatus(new AlluxioURI("/testFile")).getPermission();
    Assert.assertEquals((short) 0755, permission);
  }

  /**
   * Test -R option for chmod recursively.
   *
   * @throws Exception
   */
  @Test
  public void chmodRecursivelyTest() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testDir/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chmod", "-R", "777", "/testDir");
    int permission = mFileSystem.getStatus(new AlluxioURI("/testDir")).getPermission();
    Assert.assertEquals((short) 0777, permission);
    permission = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getPermission();
    Assert.assertEquals((short) 0777, permission);
    mFsShell.run("chmod", "-R", "755", "/testDir");
    permission = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getPermission();
    Assert.assertEquals((short) 0755, permission);
  }
}
