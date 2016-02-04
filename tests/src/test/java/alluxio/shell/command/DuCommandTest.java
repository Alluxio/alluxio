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

import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.exception.ExceptionMessage;
import alluxio.shell.AbstractTfsShellTest;

/**
 * Test for du command.
 */
public class DuCommandTest extends AbstractTfsShellTest {
  @Test
  public void duTest() throws IOException {
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testDir/testFileB",
        WriteType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testDir/testDir/testFileC",
        WriteType.MUST_CACHE, 30);

    String expected = "";
    // du a non-existing file
    mFsShell.run("du", "/testRoot/noneExisting");
    expected += ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/testRoot/noneExisting") + "\n";
    // du a file
    mFsShell.run("du", "/testRoot/testFileA");
    expected += "/testRoot/testFileA is 10 bytes\n";
    // du a folder
    mFsShell.run("du", "/testRoot/testDir");
    expected += "/testRoot/testDir is 50 bytes\n";
    Assert.assertEquals(expected, mOutput.toString());
  }
}
