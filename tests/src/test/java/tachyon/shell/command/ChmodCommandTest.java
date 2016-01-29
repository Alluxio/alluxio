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

package tachyon.shell.command;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.client.FileSystemTestUtils;
import tachyon.client.WriteType;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;

/**
 * Tests for chmod command.
 */
public class ChmodCommandTest extends AbstractTfsShellTest {

  @Test
  public void chmodTest() throws IOException, TachyonException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chmod", "777", "/testFile");
    int permission = mFileSystem.getStatus(new TachyonURI("/testFile")).getPermission();
    Assert.assertEquals((short) 0777, permission);
    mFsShell.run("chmod", "755", "/testFile");
    permission = mFileSystem.getStatus(new TachyonURI("/testFile")).getPermission();
    Assert.assertEquals((short) 0755, permission);
  }

  @Test
  public void chmodRecursivelyTest() throws IOException, TachyonException {
    clearLoginUser();
    FileSystemTestUtils.createByteFile(mFileSystem, "/testDir/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("chmod", "-R", "777", "/testDir");
    int permission = mFileSystem.getStatus(new TachyonURI("/testDir")).getPermission();
    Assert.assertEquals((short) 0777, permission);
    permission = mFileSystem.getStatus(new TachyonURI("/testDir/testFile")).getPermission();
    Assert.assertEquals((short) 0777, permission);
    mFsShell.run("chmod", "-R", "755", "/testDir");
    permission = mFileSystem.getStatus(new TachyonURI("/testDir/testFile")).getPermission();
    Assert.assertEquals((short) 0755, permission);
    mFsShell.run("chmod", "777", "/testDir", "-R");
    permission = mFileSystem.getStatus(new TachyonURI("/testDir/testFile")).getPermission();
    Assert.assertEquals((short) 0777, permission);
  }

}
