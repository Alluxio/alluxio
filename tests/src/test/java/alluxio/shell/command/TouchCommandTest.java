/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for touch command.
 */
public class TouchCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void touchTest() throws IOException, AlluxioException {
    String[] argv = new String[] {"touch", "/testFile"};
    mFsShell.run(argv);
    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/testFile"));
    Assert.assertNotNull(status);
    Assert.assertEquals(getCommandOutput(argv), mOutput.toString());
    Assert.assertFalse(status.isFolder());
  }

  @Test
  public void touchTestWithFullURI() throws IOException, AlluxioException {
    String alluxioURI = "alluxio://" + mLocalAlluxioCluster.getMasterHostname() + ":"
        + mLocalAlluxioCluster.getMasterPort() + "/destFileURI";
    // when
    String[] argv = new String[] {"touch", alluxioURI};
    mFsShell.run(argv);
    // then
    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/destFileURI"));
    Assert.assertNotNull(status);
    Assert.assertEquals(getCommandOutput(argv), mOutput.toString());
    Assert.assertFalse(status.isFolder());
  }
}
