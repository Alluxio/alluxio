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
 * Tests for chown command.
 */
public class ChownCommandTest extends AbstractAlluxioShellTest {

  @Test
  public void chownTest() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chown", "user1", "/testFile");
    String owner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getUserName();
    Assert.assertEquals("user1", owner);
    mFsShell.run("chown", "user2", "/testFile");
    owner = mFileSystem.getStatus(new AlluxioURI("/testFile")).getUserName();
    Assert.assertEquals("user2", owner);
  }

  /**
   * Test -R option for chown recursively.
   *
   * @throws Exception
   */
  @Test
  public void chownRecursiveTest() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testDir/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chown", "-R", "user1", "/testDir");
    String owner = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getUserName();
    Assert.assertEquals("user1", owner);
    owner = mFileSystem.getStatus(new AlluxioURI("/testDir")).getUserName();
    Assert.assertEquals("user1", owner);
    mFsShell.run("chown", "-R", "user2", "/testDir");
    owner = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getUserName();
    Assert.assertEquals("user2", owner);
  }
}
