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
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for touch command.
 */
public final class TouchCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void touch() throws IOException, AlluxioException {
    String[] argv = new String[] {"touch", "/testFile"};
    sFsShell.run(argv);
    URIStatus status = sFileSystem.getStatus(new AlluxioURI("/testFile"));
    Assert.assertNotNull(status);
    Assert.assertEquals(getCommandOutput(argv), mOutput.toString());
    Assert.assertFalse(status.isFolder());
  }

  @Test
  public void touchTestWithFullURI() throws IOException, AlluxioException {
    String alluxioURI =
        "alluxio://" + sLocalAlluxioCluster.getHostname() + ":" + sLocalAlluxioCluster
            .getMasterRpcPort() + "/destFileURI";
    // when
    String[] argv = new String[] {"touch", alluxioURI};
    sFsShell.run(argv);
    // then
    URIStatus status = sFileSystem.getStatus(new AlluxioURI("/destFileURI"));
    Assert.assertNotNull(status);
    Assert.assertEquals(getCommandOutput(argv), mOutput.toString());
    Assert.assertFalse(status.isFolder());
  }
}
