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
 * Tests for chgrp command.
 */
public class ChgrpCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void chgrpTest() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chgrp", "group1", "/testFile");
    String group = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroupName();
    Assert.assertEquals("group1", group);
    mFsShell.run("chgrp", "group2", "/testFile");
    group = mFileSystem.getStatus(new AlluxioURI("/testFile")).getGroupName();
    Assert.assertEquals("group2", group);
  }

  /**
   * Tests -R option for chgrp recursively.
   *
   * @throws Exception
   */
  @Test
  public void chgrpRecursiveTest() throws IOException, AlluxioException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testDir/testFile", WriteType.MUST_CACHE, 10);
    // "chgrp -R group1 /testDir" should apply to both dir and child file
    mFsShell.run("chgrp", "-R", "group1", "/testDir");
    String group = mFileSystem.getStatus(new AlluxioURI("/testDir")).getGroupName();
    Assert.assertEquals("group1", group);
    group = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getGroupName();
    Assert.assertEquals("group1", group);
    // chgrp to another group.
    mFsShell.run("chgrp", "-R", "group2", "/testDir");
    group = mFileSystem.getStatus(new AlluxioURI("/testDir/testFile")).getGroupName();
    Assert.assertEquals("group2", group);
  }
}
