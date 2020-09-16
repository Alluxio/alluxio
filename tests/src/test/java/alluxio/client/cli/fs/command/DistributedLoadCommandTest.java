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

import alluxio.AlluxioURI;
import alluxio.cli.fs.command.DistributedLoadCommand;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.grpc.WritePType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test for {@link DistributedLoadCommand}.
 */
public final class DistributedLoadCommandTest extends AbstractFileSystemShellTest {
  @Test
  public void loadDir() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testFileA", WritePType.THROUGH,
        10);
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testRoot/testFileB", WritePType.MUST_CACHE, 10);
    AlluxioURI uriA = new AlluxioURI("/testRoot/testFileA");
    AlluxioURI uriB = new AlluxioURI("/testRoot/testFileB");

    URIStatus statusA = sFileSystem.getStatus(uriA);
    URIStatus statusB = sFileSystem.getStatus(uriB);
    Assert.assertNotEquals(100, statusA.getInMemoryPercentage());
    Assert.assertEquals(100, statusB.getInMemoryPercentage());
    // Testing loading of a directory
    sFsShell.run("distributedLoad", "/testRoot");
    statusA = sFileSystem.getStatus(uriA);
    statusB = sFileSystem.getStatus(uriB);
    Assert.assertEquals(100, statusA.getInMemoryPercentage());
    Assert.assertEquals(100, statusB.getInMemoryPercentage());
  }

  @Test
  public void loadFile() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.THROUGH, 10);
    AlluxioURI uri = new AlluxioURI("/testFile");
    URIStatus status = sFileSystem.getStatus(uri);
    Assert.assertNotEquals(100, status.getInMemoryPercentage());
    // Testing loading of a single file
    sFsShell.run("distributedLoad", "/testFile");
    status = sFileSystem.getStatus(uri);
    Assert.assertEquals(100, status.getInMemoryPercentage());
  }
}
