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
