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

import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.ExceptionMessage;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.grpc.WritePType;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for du command.
 */
public final class DuCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void du() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testFileA",
        WritePType.MUST_CACHE, 0);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testFileB",
        WritePType.MUST_CACHE, 21243);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDir/testFileC",
        WritePType.THROUGH, 9712654);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDir/testDir/testFileD",
        WritePType.THROUGH, 532982);

    String expected = "";

    // du a non-existing file
    sFsShell.run("du", "/testRoot/noneExisting");
    expected += "File Size     In Alluxio       Path\n";
    expected += ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/testRoot/noneExisting") + "\n";
    Assert.assertEquals(expected, mOutput.toString());

    // du a folder
    mOutput.reset();
    sFsShell.run("du", "/testRoot/");
    expected = "File Size     In Alluxio       Path\n"
        + "532982        0 (0%)           /testRoot/testDir/testDir/testFileD\n"
        + "9712654       0 (0%)           /testRoot/testDir/testFileC\n"
        + "0             0 (0%)           /testRoot/testFileA\n"
        + "21243         21243 (100%)     /testRoot/testFileB\n";
    Assert.assertEquals(expected, mOutput.toString());

    // du a folder with options
    mOutput.reset();
    sFsShell.run("du", "-h", "-s", "/testRoot/testDir");
    expected = "File Size     In Alluxio       Path\n"
        + "9.77MB        0B (0%)          /testRoot/testDir\n";
    Assert.assertEquals(expected, mOutput.toString());

    mOutput.reset();
    sFsShell.run("du", "-h", "-s", "--memory", "/testRoot");
    expected = "File Size     In Alluxio       In Memory        Path\n"
        + "9.79MB        20.75KB (0%)     20.75KB (0%)     /testRoot\n";
    Assert.assertEquals(expected, mOutput.toString());

    String workerHostName = sLocalAlluxioCluster.getWorkerAddress().getHost();
    String format;

    mOutput.reset();
    format = "%-13s %-16s %-25s %s";
    sFsShell.run("du", "-h", "-s", "-g", "/testRoot");
    expected = "File Size     In Alluxio       Worker Host Name          Path\n"
            + "9.79MB        20.75KB (0%)     total                     /testRoot\n"
            + String.format(format, "", "20.75KB", workerHostName, "")
            + "\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void duWildcard() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDir1/testFileA",
        WritePType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDir2/testFileB",
        WritePType.THROUGH, 20);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDir2/testNotIncludeFile",
        WritePType.MUST_CACHE, 30);

    sFsShell.run("du", "/testRoot/*/testFile*");
    String expected = "File Size     In Alluxio       Path\n"
        + "10            10 (100%)        /testRoot/testDir1/testFileA\n"
        + "20            0 (0%)           /testRoot/testDir2/testFileB\n";

    sFsShell.run("du", "-h", "-s", "--memory", "/testRoot/*");
    expected += "File Size     In Alluxio       In Memory        Path\n"
        + "10B           10B (100%)       10B (100%)       /testRoot/testDir1\n"
        + "50B           30B (60%)        30B (60%)        /testRoot/testDir2\n";
    Assert.assertEquals(expected, mOutput.toString());
  }
}
