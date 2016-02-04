/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.shell.command;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import alluxio.AlluxioURI;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;

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
   * Test -R option for chgrp recursively.
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
