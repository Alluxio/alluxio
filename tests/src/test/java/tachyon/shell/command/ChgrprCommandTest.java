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
 * Tests for chgrpr command.
 */
public class ChgrprCommandTest extends AbstractTfsShellTest {

  @Test
  public void chgrprTest() throws IOException, TachyonException {
    clearLoginUser();
    mFsShell.run("mkdir", "/testFolder1");
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFolder1/testFile", WriteType.MUST_CACHE,
        10);
    mFsShell.run("chgrpr", "group1", "/testFolder1");
    String group = mFileSystem.getStatus(new TachyonURI("/testFolder1/testFile")).getGroupName();
    Assert.assertEquals("group1", group);
    mFsShell.run("chgrpr", "group2", "/testFolder1");
    group = mFileSystem.getStatus(new TachyonURI("/testFolder1/testFile")).getGroupName();
    Assert.assertEquals("group2", group);
  }
}
