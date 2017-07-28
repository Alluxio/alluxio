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

import alluxio.client.WriteType;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.ExceptionMessage;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for du command.
 */
public final class DuCommandIntegrationTest extends AbstractAlluxioShellTest {
  @Test
  public void du() throws Exception {
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
