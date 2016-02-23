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
 * Tests for chownr command.
 */
public class ChownrCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void chownrTest() throws IOException, AlluxioException {
    clearLoginUser();
    mFsShell.run("mkdir", "/testFolder1");
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFolder1/testFile", WriteType.MUST_CACHE,
        10);
    mFsShell.run("chownr", "user1", "/testFolder1");
    String owner = mFileSystem.getStatus(new AlluxioURI("/testFolder1/testFile")).getUserName();
    Assert.assertEquals("user1", owner);
    mFsShell.run("chownr", "user2", "/testFolder1");
    owner = mFileSystem.getStatus(new AlluxioURI("/testFolder1/testFile")).getUserName();
    Assert.assertEquals("user2", owner);
  }
}
