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

package alluxio.client.cli.fs.command;

import static org.junit.Assert.assertEquals;

import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.ExceptionMessage;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.grpc.WritePType;

import org.junit.Test;

/**
 * Tests for count command.
 */
public final class CountCommandTest extends AbstractFileSystemShellTest {
  @Test
  public void countNotExist() throws Exception {
    int ret = sFsShell.run("count", "/NotExistFile");
    assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/NotExistFile") + "\n",
        mOutput.toString());
    assertEquals(-1, ret);
  }

  @Test
  public void count() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testFileA",
        WritePType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDir/testFileB",
        WritePType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testFileB",
        WritePType.MUST_CACHE, 30);

    //count a folders
    sFsShell.run("count", "/testRoot");
    String expected = "";
    String format = "%-25s%-25s%-15s\n";
    expected += String.format(format, "File Count", "Folder Count", "Folder Size");
    expected += String.format(format, 3, 2, 60);

    //count a folders
    sFsShell.run("count", "-h", "/testRoot");
    expected += String.format(format, "File Count", "Folder Count", "Folder Size");
    expected += String.format(format, 3, 2, "60B");
    assertEquals(expected, mOutput.toString());
  }
}
