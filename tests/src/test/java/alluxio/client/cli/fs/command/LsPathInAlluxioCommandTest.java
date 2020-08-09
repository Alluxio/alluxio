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

import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.grpc.WritePType;

import org.junit.Test;

/**
 * Tests for lsPathInAlluxio command.
 */
public class LsPathInAlluxioCommandTest extends AbstractFileSystemShellTest {

  @Test
  public void lsPathInAlluxio() throws Exception {
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testFileA", WritePType.MUST_CACHE, 50, 50);
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testFileZ", WritePType.MUST_CACHE, 10, 10);
    FileSystemTestUtils
            .createByteFile(sFileSystem, "/testRoot/testLongFile", WritePType.MUST_CACHE, 100, 100);

    String workerHost = sLocalAlluxioCluster.getWorkerAddress().getHost();

    String expected = "";
    sFsShell.run("lsPathInAlluxio", "/testRoot");
    expected += String.format("%-25s %s\n", "Worker Host Name", "In Alluxio");
    expected += String.format("%-25s %s", workerHost, 160);

    assertEquals(expected, mOutput.toString());

    expected = "";
    sFsShell.run("lsPathInAlluxio", "-h", "/testRoot");
    expected += String.format("%-25s %s\n", "Worker Host Name", "In Alluxio");
    expected += String.format("%-25s %s", workerHost, "160B");

    assertEquals(expected, mOutput.toString());
  }
}
