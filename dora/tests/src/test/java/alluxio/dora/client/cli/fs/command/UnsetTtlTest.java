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

package alluxio.dora.client.cli.fs.command;

import static org.junit.Assert.assertEquals;

import alluxio.dora.AlluxioURI;
import alluxio.dora.Constants;
import alluxio.dora.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.dora.client.file.FileSystemTestUtils;
import alluxio.grpc.WritePType;

import org.junit.Test;

/**
 * Test for unsetTtl command.
 */
public final class UnsetTtlTest extends AbstractFileSystemShellTest {
  @Test
  public void unsetTtl() throws Exception {
    String filePath = "/testFile";
    AlluxioURI uri = new AlluxioURI("/testFile");
    FileSystemTestUtils.createByteFile(sFileSystem, filePath, WritePType.MUST_CACHE, 1);
    assertEquals(Constants.NO_TTL, sFileSystem.getStatus(uri).getTtl());

    // unsetTTL on a file originally with no TTL will leave the TTL unchanged.
    assertEquals(0, sFsShell.run("unsetTtl", filePath));
    assertEquals(Constants.NO_TTL, sFileSystem.getStatus(uri).getTtl());

    long ttl = 1000L;
    assertEquals(0, sFsShell.run("setTtl", filePath, String.valueOf(ttl)));
    assertEquals(ttl, sFileSystem.getStatus(uri).getTtl());
    assertEquals(0, sFsShell.run("unsetTtl", filePath));
    assertEquals(Constants.NO_TTL, sFileSystem.getStatus(uri).getTtl());
  }
}
